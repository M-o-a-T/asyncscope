"""
This library implements scoped taskgroups.

Large(r) programs consist of building blocks which depend on each other.
Crucially, these blocks may get started in FIFO order: the main program
starts a mid-level support task which starts a low-level connection.
In order to cleanly terminate this, the main program must be cancelled
first.

Those building blocks and their dependencies typically form a DAG. However,
we're not concerned with keeping as much of the support code running after
the main program has ended. Thus a linear ordering is sufficient.

Usage:

Wrap your main async code with ``async with main_scope(): ...``.

Start a service task (i.e. something you depend on) with
``asyncscope.spawn_service``. Start a subtask which should run in parallel
and which you're going to wait on with ``asyncscope.spawn``.

Scope objects are handled in the background, though you are free to
create one.

"""
from __future__ import annotations

import logging
from collections import defaultdict
from concurrent.futures import CancelledError
from contextlib import asynccontextmanager
from contextvars import ContextVar
from functools import partial
from typing import Any, Dict, Set

import anyio
import anyio.abc

_scope = ContextVar("scope", default=None)


class _ScopeProxy:
    def get(self):
        "return the actual scope contextvar"
        return _scope.get()

    def __getattr__(self, k):
        return getattr(_scope.get(), k)

    def thread_reset(self):
        "clear scope var for use w/ multithreading"
        _scope.set(None)

scope = _ScopeProxy()


class Scope:
    """
    A single scope, encapsulating some potentially-long-running service
    that might be used by multiple independent tasks / other scopes.
    """

    _ctx_ = None

    # unnamed scopes. Classvar.
    _id = 0

    # key: the scopes I depend on
    # val: how often I do – might have been added more than once
    _next: Dict[Scope, int] = None

    # the sets I depend on
    _prev: Set[Scope] = None

    # signal that this scope is closed
    _done: anyio.abc.Event = None

    # caller, to reset the contextcar
    _scope = None

    # Taskgroup that controls the jobs running in this scope
    _tg: anyio.abc.TaskGroup = None

    # Main scope which contains all linked scopes
    _set: ScopeSet = None

    # my name
    _name: str = None
    _new: bool = False

    # Data storage for look-up by other modules
    _data: Any = None

    # Service startup caused an exception? owch.
    _error: Exception = None

    # Signal that the data storage is ready
    _data_lock: anyio.abc.Lock = None

    # Signal for controlled shutdown via no_more_dependents
    _no_more: anyio.abc.Event = None
    # if None, the scope's taskgroup is cancelled instead

    def __init__(self, scopeset: ScopeSet, name: str, new: bool = False):
        self._next = defaultdict(lambda: 0)
        self._prev = set()
        self._set = scopeset
        self._name = name
        self._done = anyio.Event()
        self._new = new
        self._data_lock = anyio.Event()
        self._set[self._name] = self

        self.logger = logging.getLogger(f"scope.{name}")

    @property
    def data(self):
        "retrieve the scope's data object"
        return self._data

    async def spawn(self, proc, *args, **kwargs):
        """
        Run a task within this scope.

        Returns:
            a cancel scope you can use to stop the task.
        """

        async def _run(proc, a, kw, *, task_status):
            """
            Helper for starting a task.

            The task status passes the task's cancel scope back to the caller.
            """
            with anyio.CancelScope() as _scope:
                task_status.started(_scope)
                try:
                    self.logger.debug("Start %s %s %s", proc, a, kw)
                    await proc(*a, **kw)
                except BaseException as exc:
                    self.logger.debug("Err %s %r", proc, exc)
                    raise
                else:
                    self.logger.debug("End %s", proc)

        return await self._tg.start(_run, proc, args, kwargs)

    def start_soon(self, proc, *args, **kwargs):
        self._tg.start_soon(partial(proc, *args, **kwargs))

    async def start(self, proc, *args, **kwargs):
        return await self._tg.start(partial(proc, *args, **kwargs))

    @property
    def cancel_scope(self):
        return self

    async def spawn_service(self, proc, *args, _as_scope=False, **kwargs):
        """
        Run 'proc' in a new service scope, i.e. one that should auto-end
        *after* the current scope terminates.

        Returns: the new scope.
        """
        Scope._id += 1
        sc_id = Scope._id

        s = await self._service(f"_dyn_{sc_id}", proc, args, kwargs)
        return s if _as_scope else s._data

    async def _service(self, name, proc, args, kwargs):
        """
        Start a service in a new scope, return the scope.
        """
        try:
            s = self._set[name]
        except KeyError:
            self.logger.debug("requires %s", name)
            s = Scope(self._set, name)
            await self._set.spawn(s, proc, *args, **kwargs)
        else:
            self.logger.debug("also requires %s", name)
        self.requires(s)

        await s._data_lock.wait()
        if s._error is None:
            self.logger.debug("%s = %r", name, s._data)
        else:
            self.logger.error("%s = %r", name, s._error)
            self.release(s, dead=True)
            raise s._error
        return s

    async def service(self, name, proc, *args, _as_scope=False, **kwargs):
        """
        Start this service as a context this scope depends on
        (if it doesn't run already).

        Returns: the data which the service registers / has registered.

        Call ``release(name)`` when you no longer need the service.
        (This will happen automatically when your scope ends.)
        """
        s = await self._service(name, proc, args, kwargs)
        return s if _as_scope else s._data

    @asynccontextmanager
    async def using_service(self, name, proc, *args, **kwargs):
        """
        This is a context manager which starts / uses this service as
        a dependent context.

        The service is started if it doesn't currently run, and stopped
        when the last user leaves its context.

        Returns: the data which the service registers / has registered.
        """
        s = await self._service(name, proc, args, kwargs)
        try:
            yield s._data
        finally:
            self.release(s)

    def lookup(self, name):
        """
        Return the data associated with some scope.

        Raises KeyError if the named scope doesn't exist or has not
        provided any data.

        Use this if you need a temporary reference to a scope which might
        depend on the current one.

        Never use this method to check whether you need to call `service`.
        """
        s = self.lookup_scope(name)
        if not s._data_lock.is_set():
            raise KeyError(name)
        scope.requires(s)
        return s._data

    def lookup_scope(self, name):
        """
        Return the scope associated with some name.

        Raises KeyError if the named scope doesn't exist.

        Use this if you need to add a dependency.
        """
        s = self._set[name]
        return s

    def register(self, data: Any):
        """
        Register some data with this scope.

        The data is returned by calls to `service` from other scopes.

        The returned data will have an _asyncscope element.
        """
        if self._data_lock.is_set():
            raise RuntimeError(f"{self !r} can't change the registration value")
        self.logger.debug("%s: obj %r", self._name, data)
        self._data = data
        data._asyncscope = scope
        self._data_lock.set()

    @asynccontextmanager
    async def _ctx(self):
        """
        The scope's context manager.
        """
        if self._scope is not None:
            raise RuntimeError("You can't enter a scope twice")
        if self._set[self._name] is not self:
            raise RuntimeError(f"Lookup of {self._name} does not match scope")

        current_scope = _scope.get()
        self._scope = _scope.set(self)
        try:
            async with anyio.create_task_group() as tg:
                self._tg = tg

                if not self._new and current_scope is not None:
                    current_scope.requires(self)

                try:
                    yield self
                finally:
                    del self._set[self._name]
                    with anyio.CancelScope(shield=True):
                        await self.cancel_immediate()

        except Exception as exc:
            if self._data_lock.is_set():
                raise
            self.logger.exception("Ugh %r", exc)
            self._error = exc
            self._data_lock.set()

        except BaseException:
            if not self._data_lock.is_set():
                self._error = CancelledError()
                self._data_lock.set()
            raise

        finally:
            _scope.reset(self._scope)
            self._scope = None

            if not self._data_lock.is_set():
                self._error = RuntimeError(
                    f"{self._name} didn't call `scope.register`!"
                )
                self._data_lock.set()
            self._done.set()
            for p in list(self._prev):
                self.release(p, dead=True)
            self._tg = None

    async def __aenter__(self):
        if self._ctx_ is not None:
            raise RuntimeError("A scope can only be used once")
        self._ctx_ = self._ctx()
        return await self._ctx_.__aenter__()  # pylint:disable=no-member  # YES IT HAS

    async def __aexit__(self, *tb):
        return await self._ctx_.__aexit__(*tb)  # pylint:disable=no-member  # YES IT HAS

    def may_not_require(self, s: Scope):
        """
        Assert that this scope doesn't require scope @s.

        Raises `RuntimeError` if it does.
        """
        seen = set()
        todo = set((self,))
        while todo:
            t = todo.pop()
            if s is t:
                raise RuntimeError(f"{self} may not require {s}")
            todo |= t._prev - seen
            seen |= todo

    def requires(self, s: Scope):
        """
        This scope requires another (already existing) scope in order to function.
        """
        if self is s:
            return
        if self._set is not s._set:
            raise RuntimeError(f"{self}/{self._set} disparate to {s}/{s._set}")
        if s in self._prev:
            assert s._next[self] > 0
            s._next[self] += 1
            return
        s.may_not_require(self)
        self._prev.add(s)
        s._next[self] += 1

    def release(self, s: Scope, dead: bool = False):
        """
        This scope no longer requires @s.

        @s is cancelled if it no longer has any dependents.

        Set @dead if the service running in scope @s is no longer useable.
        """
        if self not in s._next:
            # happens when somebody called with dead=True
            return
        assert s._next[self] > 0
        if not dead:
            s._next[self] -= 1
            if s._next[self]:
                self.logger.debug("release %s, in use %d", s._name, s._next[self])
                return
            self.logger.debug("release %s, closing")
        else:
            self.logger.debug("release %s, dead, in use %d", s._name, s._next[self])

        s._released(self)

    def _released(self, s: Scope):
        """
        @s no longer requires me.
        """
        del self._next[s]
        s._prev.remove(self)
        if not self._next:
            self.no_more()

    def no_more(self):
        self.logger.debug("No more users")
        if self._no_more is None:
            self.cancel()
        else:
            self._no_more.set()

    @property
    def dependents(self):
        """
        Iterator that returns all scopes which (transitively) depend on this one.

        Ordered top-down.
        """
        seen = set()

        def deps(s):
            if s in seen:
                return
            seen.add(s)
            for n in set(s._next) - seen:
                yield from deps(n)
            yield s

        for n in list(self._next.keys()):
            yield from deps(n)

    async def no_more_dependents(self):
        """
        Wait until all of your dependents are gone.

        Call this if you want to clean up in a controlled way instead of
        getting cancelled.

        You *must* terminate your scope if this returns!
        """
        if not self._next:
            return
        try:
            self._no_more = anyio.Event()
            self.logger.debug("Wait No more users")
            await self._no_more.wait()
        except BaseException as exc:
            self.logger.debug("Wait No more users: %r", exc)
        else:
            self.logger.debug("Wait No more users: OK")
        finally:
            self._no_more = None
            del self._data._asyncscope
            del self._data

    async def cancel_dependents(self):
        """
        This will cancel, and wait for, all scope(s) that depend on this one.

        XXX cancellation is strictly sequential. Some parallelization might
        be a good idea.
        """
        self.logger.debug("Cancel dependents")
        for s in self.dependents:
            s.no_more()
            await s.wait()
        self.logger.debug("Cancel dependents done")

    async def cancel_immediate(self):
        """
        Cancel this scope.

        This will cancel all scopes that depend on this one without waiting
        for them to terminate.
        """
        self.logger.debug("Cancel Immediate")
        for s in self.dependents:
            s.cancel()
        self.cancel()

    def cancel(self):
        """
        Cancel this scope.
        """
        if self._tg:
            self.logger.debug("Cancelled")
            self._tg.cancel_scope.cancel()

    async def wait(self):
        """
        Wait until this scope has terminated.
        """
        self.logger.debug("Wait for end")
        await self._done.wait()
        self.logger.debug("End")

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return id(self) == id(other)

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self._name)

    @property
    def name(self):
        return self._name

    def subscope(self):
        sid = self._set._seq
        self._set._seq += 1
        return Scope(self, f"_sub_{sid}")


class ScopeSet:
    """
    This class is the container for the `main_scope` context manager.
    """

    _tg = None
    _ctx_ = None
    _seq = 0

    def __init__(self, name):
        self.name = name
        self.logger = logging.getLogger(f"scope.{name}")
        self._scopes: Dict[str, Scope] = dict()

    async def spawn(self, s: Scope, proc, *args, **kwargs):
        """
        Run 'proc' in the given scope.
        The scope must be new and the current scope must already depend on it.
        """
        scope.logger.debug("Spawn %r: %r %r %r", s, proc, args, kwargs)

        async def _service(s, proc, args, kwargs, *, task_status):
            with anyio.CancelScope(shield=True):
                # Shielded because cancellation is managed by the scope
                async with s._ctx():
                    task_status.started()
                    await proc(*args, **kwargs)

        await self._tg.start(_service, s, proc, args, kwargs)

    def __getitem__(self, key):
        if isinstance(key, Scope):
            key = key._name
        return self._scopes[key]

    def __setitem__(self, key, value):
        if isinstance(key, Scope):
            key = key._name
        self._scopes[key] = value

    def __delitem__(self, key):
        if isinstance(key, Scope):
            key = key._name
        try:
            del self._scopes[key]
        except KeyError:
            pass

    def __contains__(self, key):
        if isinstance(key, Scope):
            key = key._name
        return key in self._scopes

    @asynccontextmanager
    async def _ctx(self):
        """
        Context manager for a new scope set
        """
        if scope.get() is not None:
            raise RuntimeError("Don't nest scopesets")
        s = Scope(self, self.name, new=True)
        s.register(self)
        async with anyio.create_task_group() as tg:
            self._tg = tg
            self._scopes[s._name] = s
            async with s:
                try:
                    yield s
                finally:
                    del self._scopes[s._name]
            pass  # end scope context
        pass  # end taskgroup

        # At this point `self._scopes` shall be empty
        if self._scopes:
            raise RuntimeError(
                "Main scope ended but not empty: "
                + " ".join(tuple(repr(x) for x in self._scopes.keys()))
            )

    async def __aenter__(self):
        if self._ctx_ is not None:
            raise RuntimeError("A ScopeSet can only be used once")
        self._ctx_ = self._ctx()
        return await self._ctx_.__aenter__()  # pylint:disable=no-member  # YES IT HAS

    async def __aexit__(self, *tb):
        return await self._ctx_.__aexit__(*tb)  # pylint:disable=no-member  # YES IT HAS

    def __repr__(self):
        return "<%s>" % (self.__class__.__name__,)


def get_scope(data: Any) -> Scope:
    """
    Retrieve the scope a data element was created in.
    """
    return data._asyncscope


@asynccontextmanager
async def main_scope(name="_main"):
    """
    This context manager provides you with a new "main" scope, i.e. one you
    can start service tasks in.
    """

    async with ScopeSet(name=name) as s:
        try:
            yield s
        finally:
            await s.cancel_dependents()  # there should not be any, but …
            s.cancel()
    pass  # end main scope
