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

import anyio
import anyio.abc
from contextvars import ContextVar
from contextlib import asynccontextmanager
from collections import defaultdict
from functools import partial
from typing import Any, Set, Dict

import logging

_scope = ContextVar("scope", default=None)


class _ScopeProxy:
    def get(self):
        return _scope.get()

    def __getattr__(self, k):
        return getattr(_scope.get(), k)


scope = _ScopeProxy()


class Scope:
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

        self._logger = logging.getLogger(f"{self._set.logger.name}.{name}")

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
                    self._logger.debug("Start %s %s %s", proc,a,kw)
                    await proc(*a, **kw)
                except BaseException as exc:
                    self._logger.debug("Err %s %r", proc,exc)
                else:
                    self._logger.debug("End %s", proc)

        return await self._tg.start(_run, proc, args, kwargs)

    def start_soon(self, proc, *args, **kwargs):
        self._tg.start_soon(partial(proc, *args, **kwargs))

    async def start(self, proc, *args, **kwargs):
        return await self._tg.start(partial(proc, *args, **kwargs))

    @property
    def cancel_scope(self):
        return self

    async def spawn_service(self, proc, *args, **kwargs):
        """
        Run 'proc' in a new service scope, i.e. one that should end *after* the
        current scope terminates.

        Special arguments:
            _name_: Name for the new scope

        Returns: the new scope.
        """
        return await self._set.spawn(proc, *args, **kwargs)

    async def _service(self, name, proc, args, kwargs):
        try:
            s = self._set[name]
        except KeyError:
            self._logger.debug("requires %s", name)
            s = await self.spawn_service(proc, *args, **kwargs, _name_=name, _by_=self)
        else:
            self._logger.debug("also requires %s", name)
            self.requires(s)
        await s._data_lock.wait()
        self._logger.debug("%s = %r", name, s._data)
        return s

    async def service(self, name, proc, *args, **kwargs):
        """
        Start this service as a context this scope depends on
        (if it doesn't run already).

        Returns: the data which the service registers / has registered.

        Call ``release(name)`` when you no longer need the service.
        (This will happen automatically when your scope ends.)
        """
        s = await self._service(name, proc, args, kwargs)
        return s._data

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
        return s._data

    def lookup_scope(self, name):
        """
        Return the scope associated with some name.

        Raises KeyError if the named scope doesn't exist.

        Use this if you need to add a dependency.
        """
        s = self._set[name]
        return s

    async def register(self, data: Any):
        """
        Register some data with this scope.

        The data is returned by calls to `service` from other scopes.
        """
        if self._data_lock.is_set():
            raise RuntimeError("You can't change the registration value")
        self._logger.debug("%s: obj %r", self._name, data)
        self._data = data
        self._data_lock.set()

    @asynccontextmanager
    async def _ctx(self):
        """
        The scope's context manager.
        """
        if self._scope is not None:
            raise RuntimeError("You can't enter a scope twice")
        try:
            self._set[self._name] = self
            current_scope = _scope.get()
            self._scope = _scope.set(self)
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
        finally:
            if self._scope is not None:  # error in setup
                _scope.reset(self._scope)
                self._data_lock.set()
                self._done.set()
                for p in list(self._prev):
                    self.release(p, dead=True)
            self._tg = None
            self._scope = None

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
                self._logger.debug("release %s, in use %d", s._name, s._next[self])
                return
            self._logger.debug("release %s, closing")
        else:
            self._logger.debug("release %s, dead, in use %d", s._name, s._next[self])

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
        self._logger.debug("No more users")
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
            self._logger.debug("Wait No more users")
            await self._no_more.wait()
        except BaseException as exc:
            self._logger.debug("Wait No more users: %r", exc)
        else:
            self._logger.debug("Wait No more users: OK")
        finally:
            self._no_more = None

    async def cancel_dependents(self):
        """
        This will cancel, and wait for, all scope(s) that depend on this one.

        XXX cancellation is strictly sequential. Some parallelization might
        be a good idea.
        """
        self._logger.debug("Cancel dependents")
        for s in self.dependents:
            s.no_more()
            await s.wait()
        self._logger.debug("Cancel dependents done")

    async def cancel_immediate(self):
        """
        Cancel this scope.

        This will cancel all scopes that depend on this one without waiting
        for them to terminate.
        """
        self._logger.debug("Cancel Immediate")
        for s in self.dependents:
            s.cancel()
        self.cancel()

    def cancel(self):
        """
        Cancel this scope.
        """
        if self._tg:
            self._logger.debug("Cancelled")
            self._tg.cancel_scope.cancel()

    async def wait(self):
        """
        Wait until this scope has terminated.
        """
        self._logger.debug("Wait for end")
        await self._done.wait()
        self._logger.debug("End")

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return id(self) == id(other)

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self._name)

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
    _main_name: str = None
    _seq = 0

    def __init__(self, name: str = None):
        self.logger = logging.getLogger(f"scope.{name}")
        self._scopes: Dict[str, Scope] = dict()
        if name is None:
            type(self)._seq += 1
            name = "_main_%d" % (type(self)._seq)
        self._main_name = name

    async def spawn(self, proc, *args, _name_: str = None, _by_: Scope = None, **kwargs):
        """
        Run 'proc' in a new scope which the current scope depends on.

        Special arguments:
            _name_: Name for the new scope
            _by_: the scope that should depend on this one

        Returns: the new scope.
        """
        s = None
        if _name_ is None:
            _name_ = proc.__name__

        async def _service(s, proc, args, kwargs, *, task_status=None):
            with anyio.CancelScope(shield=True):
                # Shielded because cancellation is managed by the scope
                async with s._ctx():
                    task_status.started()
                    await proc(*args, **kwargs)

        s = Scope(self, _name_)
        self._scopes[_name_] = s
        if _by_ is not None:
            _by_.requires(s)
        await self._tg.start(_service, s, proc, args, kwargs)
        return s

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
        s = Scope(self, "_main", new=True)
        async with anyio.create_task_group() as tg:
            self._tg = tg
            async with s._ctx():
                self._scopes[s._name] = s
                try:
                    yield s
                finally:
                    del self._scopes[s._name]
            pass  # end scope context
        pass  # end taskgroup

        # At this point `self._scopes` shall be empty
        if self._scopes:
            raise RuntimeError("Scope st ended but not empty")

    async def __aenter__(self):
        if self._ctx_ is not None:
            raise RuntimeError("A ScopeSet can only be used once")
        self._ctx_ = self._ctx()
        return await self._ctx_.__aenter__()

    async def __aexit__(self, *tb):
        return await self._ctx_.__aexit__(*tb)  # pylint:disable=no-member  # YES IT HAS

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self._main_name)


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

