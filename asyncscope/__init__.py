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

import sys
import logging
from collections import defaultdict
from concurrent.futures import CancelledError
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from functools import partial
from typing import Any, Dict, Set

import anyio
import anyio.abc

_scope = ContextVar("scope", default=None)


class ScopeDied(RuntimeError):
    pass

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


class _Scope:
    """
    Common data for both normal and usage scopes.
    """

    _ctx_ = None

    # Main scope which contains all linked scopes
    _set: ScopeSet = None

    # caller, to reset the contextvar
    _scope = None

    # key: the scopes I depend on
    _requires: Set[Scope] = None

    # unique number, for subscope names
    _seqnr = 0

    # my name
    _name: str = None

    cancel_called:bool = False

    def __init__(self, scopeset: ScopeSet, name:str):
        self._set = scopeset
        self._name = name
        self._requires = set()
        self._exc = []

        self.logger = logging.getLogger(name)

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self._name)

    @property
    def name(self):
        return self._name

    @property
    def seqnr(self):
        """
        Counter property.
        """
        self._seqnr += 1
        return self._seqnr

    def release_required(self):
        """
        Drop all requirements
        """
        for p in list(self._requires):
            self.release(p, dead=True)

    def release(self, s: Scope, dead: bool = False):
        """
        This scope no longer uses @s.

        @s is cancelled if it no longer has any dependents.

        Set @dead if the service running in scope @s is no longer useable.
        """
        if s.release_user(self, dead):
            return
        self._requires.remove(s)
        s._released(self)

    def release_user(self, s, dead:bool = False):
        """
        Forget this user.

        No-Op here, overridden in `Scope`.
        """
        pass

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
            todo |= t._requires - seen
            seen |= todo

    def requires(self, s: Scope):
        """
        This scope requires another (already existing) scope in order to function.
        """
        if self is s:
            return
        if self._set is not s._set:
            raise RuntimeError(f"{self}/{self._set} disparate to {s}/{s._set}")
        if s in self._requires:
            assert s._users[self] > 0
            s._users[self] += 1
            return
        s.may_not_require(self)
        self._requires.add(s)
        s._users[self] += 1

    def _released(self, s: Scope):
        """
        @s no longer requires me.
        """
        pass

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
            s.logger.debug("= %r", s._data)
        else:
            s.logger.error("= %r", s._error)
            self.release(s, dead=True)
            raise s._error
        return s

    async def spawn_service(self, proc, *args, _as_scope=False, **kwargs):
        """
        Run 'proc' in a new service scope, i.e. one that should auto-end
        *after* the current scope terminates.

        Returns: the new scope.
        """
        s = await self._service(f"{self._name}._{self.seqnr}", proc, args, kwargs)
        return s if _as_scope else s._data

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

    @asynccontextmanager
    async def using_scope(self, name=None):
        """
        This is a context manager which allows nesting contexts.

        This context raises a `ScopeDied` exception if a scope *it* started
        exited.
        """

        async with UseScope(self._set, name=name) as sw:
            try:
                yield sw
            except ScopeDied as exc:
                if not self._exc:
                    raise
                self.logger.debug(f"Error: {exc!r} in {self.name}", exc_info=exc)
                self._exc.append(exc)
            except Exception as exc:
                self.logger.debug(f"Error: {exc!r} in {self.name}", exc_info=exc)
                self._exc.append(exc)
            finally:
                if self._exc:
                    raise ExceptionGroup(self.name, self._exc)
                cc = sw.cancel_called
        if cc:
            raise ScopeDied(self)

    async def spawn(self, proc, *args, **kwargs):
        """
        Run a task within this scope.
        The task will be auto-cancelled when the scope's main task exits.

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

    def cancel(self, killed:bool = False):
        """
        Cancel this scope.
        """
        self.logger.debug("Cancelled")
        self.cancel_called = True
        if self._tg:
            self._tg.cancel_scope.cancel()

    def _sig_no_users(self):
        """"""
        if self._no_users is None:
            self.logger.debug("No more users, cancel task")
            self.cancel()
        else:
            self.logger.debug("No more users, set no_more flag")
            self._no_users.set()


    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return id(self) == id(other)


class Scope(_Scope):
    """
    A single scope, encapsulating some potentially-long-running service
    that might be used by multiple independent tasks / other scopes.
    """

    # Data storage for look-up by other modules
    _data: Any = None

    # Service startup caused an exception? owch.
    _error: Exception = None

    # Signal that the data storage is ready
    _data_lock: anyio.abc.Lock = None

    # signal that this scope is closed
    _done: anyio.abc.Event = None

    # Taskgroup that controls the jobs running in this scope
    _tg: anyio.abc.TaskGroup = None

    # the scopes depending on me
    # val: how often they do – might have been added more than once
    _users: Dict[Scope, int] = None

    # Signal for controlled shutdown via wait_no_users
    _no_users: anyio.abc.Event = None
    # if None, the scope's taskgroup is cancelled instead

    def __init__(self, scopeset: ScopeSet, name: str):
        super().__init__(scopeset, name)
        self._done = anyio.Event()
        self._data_lock = anyio.Event()
        self._set[self._name] = self
        self._users = defaultdict(lambda: 0)

    @property
    def data(self):
        "retrieve the scope's data object"
        return self._data

    def _released(self, s: Scope):
        if not self._users:
            self._sig_no_users()

    def register(self, data: Any):
        """
        Register some data with this scope.

        The data is returned by calls to `service` from other scopes.

        The returned data will have an _asyncscope element, if possible.
        """
        if self._data_lock is None:
            raise RuntimeError(f"Don't call from a 'using_service' block")

        if self._data_lock.is_set():
            raise RuntimeError(f"{self !r} can't change the registration value")
        self.logger.debug("obj %r", data)
        self._data = data
        try:
            data._asyncscope = scope
        except AttributeError:
            pass
        self._data_lock.set()

    @asynccontextmanager
    async def _ctx(self, current_scope=None):
        """
        The scope's context manager.
        """
        if self._scope is not None:
            raise RuntimeError("You can't enter a scope twice")
        if self._set[self._name] is not self:
            raise RuntimeError(f"Lookup of {self._name} does not match scope")

        if current_scope is None:
            current_scope = _scope.get()
        self._scope = _scope.set(self)
        try:
            async with anyio.create_task_group() as tg:
                self._tg = tg

                if current_scope is not None:
                    current_scope.requires(self)

                try:
                    yield self
                finally:
                    del self._set[self._name]
                    self.cancel_immediate()

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
            if not self._data_lock.is_set():
                self._error = RuntimeError(
                    f"{self._name} didn't call `scope.register`!"
                )
                self._data_lock.set()
            self._done.set()

            _scope.reset(self._scope)
            self._scope = None
            self.release_required()
            self._tg = None

    async def __aenter__(self):
        if self._ctx_ is not None:
            raise RuntimeError("A scope can only be used once")
        self._ctx_ = self._ctx()
        return await self._ctx_.__aenter__()  # pylint:disable=no-member  # YES IT HAS

    async def __aexit__(self, *tb):
        return await self._ctx_.__aexit__(*tb)  # pylint:disable=no-member  # YES IT HAS


    async def wait_no_users(self):
        """
        Wait until all of your users are gone.

        Call this if you want to clean up in a controlled way instead of
        getting cancelled.

        You *must* terminate your scope if this returns!
        """
        if not self._users:
            return
        try:
            self._no_users = anyio.Event()
            self.logger.debug("Wait until No more users")
            await self._no_users.wait()
        except BaseException as exc:
            self.logger.debug("Wait until No more users: %r", exc)
            raise
        else:
            self.logger.debug("Wait until No more users: OK")
        finally:
            self._no_users = None

        try:
            del self._data._asyncscope
        except AttributeError:
            pass
        try:
            del self._data
        except AttributeError:
            pass

    # compatibility
    def no_more_dependents(self):
        return self.wait_no_users()


    def release_user(self, s, dead:bool = False) -> bool:
        """
        Forget this user.

        Return True if the user should not be dropped, either because it
        already was or because the use count is still >0.
        """
        if s not in self._users:
            # happens when called previously, with dead=True
            return True
        assert self._users[s] > 0

        if dead:
            self.logger.debug("releasing %s, dead, in use %d", s._name, self._users[s])
        else:
            self._users[s] -= 1
            if self._users[s]:
                self.logger.debug("releasing %s, still in use %d", s._name, self._users[s])
                return True
            self.logger.debug("releasing %s, closing", s._name)
        del self._users[s]
        return False


    @property
    def all_users(self):
        """
        Iterator that returns all scopes which (transitively) depend on this one.

        Ordered top-down, skips seen.
        """
        seen = set()

        def deps(s):
            if s in seen:
                return
            seen.add(s)
            for n in s._users_set - seen:
                yield from deps(n)
            yield s

        for s in list(self._users.keys()):
            yield from deps(s)

    @property
    def _users_set(self):
        return set(self._users)

    def cancel_immediate(self) -> bool:
        """
        Cancel this scope, and all scopes that use it.

        Returns a flag signalling that there were any users.
        """
        self.logger.debug("Cancel Immediate")
        done = False
        for s in self.all_users:
            s.cancel()
            done = True
        self.cancel()
        return done

    async def wait(self):
        """
        Wait until this scope has terminated.
        """
        self.logger.debug("Wait for end")
        await self._done.wait()
        self.logger.debug("End")


class UseScope(_Scope):
    # Marker for `cancel_immediate` that teaches a "using_service" context
    # manager that a cancellation needs to cause an error
    _killed:bool = False

    # Exceptions seen in scopes we use
    _exc:list[Exception] = None

    def __init__(self, scopeset:ScopeSet, name:str = None):
        if name is None:
            name = f"{scope._name}._{scope.seqnr}"
        super().__init__(scopeset, name)


    @property
    def _users_set(self):
        # doesn't have any
        return set()

    @asynccontextmanager
    async def _ctx(self):
        """
        The UseScope's context manager.
        """
        # This is a bit simpler than Scope._ctx because it doesn't have
        # multiple users.

        if self._scope is not None:
            raise RuntimeError("You can't enter a scope twice")
        self._scope = _scope.set(self)

        try:
            async with anyio.create_task_group() as tg:
                self._tg = tg

                try:
                    yield self
                finally:
                    self._tg.cancel_scope.cancel()

        finally:
            _scope.reset(self._scope)
            self._scope = None
            self.release_required()
            self._tg = None

    def cancel(self, exc:Exception = None):
        """
        Cancel this scope.
        """
        super().cancel()
        if exc:
            self._exc.append(exc)

    async def __aenter__(self):
        if self._ctx_ is not None:
            raise RuntimeError("A scope can only be used once")
        self._ctx_ = self._ctx()
        return await self._ctx_.__aenter__()  # pylint:disable=no-member  # YES IT HAS

    async def __aexit__(self, *tb):
        return await self._ctx_.__aexit__(*tb)  # pylint:disable=no-member  # YES IT HAS


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

        async def _service(sc, s, proc, args, kwargs, *, task_status):
            with anyio.CancelScope(shield=True):
                # Shielded because cancellation is managed by the scope
                async with s._ctx(current_scope=sc):
                    task_status.started()
                    await proc(*args, **kwargs)

        sc = _scope.get()
        await self._sc._tg.start(_service, sc, s, proc, args, kwargs)

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
            raise RuntimeError("Don't nest scopesets!")
        async with UseScope(self, name=f"{self.name}._main") as s:
            self._sc = s
            try:
                async with UseScope(self, name=self.name) as si:
                    yield s
            except Exception as exc:
                s._exc.append(exc)

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
            s.cancel()
            s.release_required()
    pass  # end main scope
