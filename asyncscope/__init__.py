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
from typing import Any, Set, Dict

scope = ContextVar("scope", default=None)


class Scope:
    _next: Set[Scope] = None
    _prev: Set[Scope] = None
    _done: anyio.abc.Event = None
    _scope = None
    # This is the taskgroup that controls the jobs running in this scope
    _tg: anyio.abc.TaskGroup = None

    # This is the taskgroup which contains all linked scopes
    _set: ScopeSet = None
    _name: str = None
    _new: bool = False

    # Data storage for look-up by other modules
    _data: Any = None
    _data_lock: anyio.abc.Lock = None

    _no_more: anyio.abc.Lock = None

    def __init__(self, scopeset: ScopeSet, name: str, new: bool = False):
        self._next = set()
        self._prev = set()
        self._set = scopeset
        self._name = name
        self._done = anyio.create_event()
        self._new = new
        self._data_lock = anyio.create_event()

    async def spawn(self, proc, *args, **kwargs):
        """
        Run a task within this scope.

        Returns:
            a cancel scope you can use to stop the task.
        """

        _scope = None

        async def _run(proc, a, kw, evt):
            """
            Helper for starting a task.

            This accepts a :class:`ValueEvent`, to pass the task's cancel scope
            back to the caller.
            """
            nonlocal _scope
            async with anyio.open_cancel_scope() as _scope:
                await evt.set()
                await proc(*a, **kw)

        evt = anyio.create_event()
        await self._tg.spawn(_run, proc, args, kwargs, evt)
        await evt.wait()
        return _scope

    async def spawn_service(self, proc, *args, **kwargs):
        """
        Run 'proc' in a new service scope, i.e. one that should end *after* the
        current scope terminates.

        Special arguments:
            _name_: Name for the new scope

        Returns: the new scope.
        """
        return await self._set.spawn(proc, *args, **kwargs)

    async def service(self, name, proc, *args, **kwargs):
        """
        Start this service as a dependent context if it doesn't run
        already.

        Returns: the data which the service registers / has registered.
        """
        try:
            s = self._set[name]
        except KeyError:
            s = await self.spawn_service(proc, *args, **kwargs, _name_=name, _by_=self)
        else:
            self.requires(s)
        await s._data_lock.wait()
        return s._data

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
        self._data = data
        await self._data_lock.set()

    @asynccontextmanager
    async def _ctx(self):
        """
        The scope's context manager.
        """
        if self._scope is not None:
            raise RuntimeError("You can't enter a scope twice")
        try:
            self._set[self._name] = self
            os = scope.get()
            self._scope = scope.set(self)
            async with anyio.create_task_group() as tg:
                self._tg = tg

                if not self._new and os is not None:
                    os.requires(self)

                try:
                    yield self
                finally:
                    del self._set[self._name]
                    async with anyio.open_cancel_scope(shield=True):
                        await self.cancel_immediate()
        finally:
            if self._scope is not None:  # error in setup
                scope.reset(self._scope)
                await self._data_lock.set()
                async with anyio.open_cancel_scope(shield=True):
                    await self._done.set()
                    for p in list(self._prev):
                        await self.release(p)
            self._tg = None
            self._scope = None

    def may_not_require(self, s: Scope):
        """
        Ensure that this scope doesn't require s to function.
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
        s.may_not_require(self)
        self._prev.add(s)
        s._next.add(self)

    async def release(self, s: Scope):
        """
        This scope no longer requires another.

        The other scope is cancelled if it no longer has any dependents.
        """
        self._prev.remove(s)
        s._next.remove(self)
        if not s._next:
            if s._no_more is None:
                await s.cancel()
            else:
                await s._no_more.set()

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
            for n in s._next - seen:
                yield from deps(n)
            yield s

        for n in list(self._next):
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
            self._no_more = anyio.create_event()
            await self._no_more.wait()
        finally:
            self._no_more = None

    async def cancel_dependents(self):
        """
        This will cancel, and wait for, all scope(s) that depend on this one.

        XXX cancellation is strictly sequential. Some parallelization might
        be a good idea.
        """
        for s in self.dependents:
            if s._no_more is None:
                await s.cancel()
            else:
                await s._no_more.set()
            await s.wait()

    async def cancel_immediate(self):
        """
        Cancel this scope.

        This will cancel all scopes that depend on this one without waiting
        for them to terminate.
        """
        for s in self.dependents:
            await s.cancel()
        await self.cancel()

    async def cancel(self):
        """
        Cancel this scope.

        Do not call directly!
        """
        if self._tg:
            await self._tg.cancel_scope.cancel()

    async def wait(self):
        """
        Wait until this scope has terminated.
        """
        await self._done.wait()

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return id(self) == id(other)

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self._name)


class ScopeSet:
    _tg = None
    _ctx_ = None
    _main_name: str = None

    def __init__(self, name: str = None):
        self._scopes: Dict[str, Scope] = dict()
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

        async def _service(s, proc, args, kwargs):
            async with anyio.open_cancel_scope(shield=True), s._ctx():
                await proc(*args, **kwargs)

        s = Scope(self, _name_)
        self._scopes[_name_] = s
        if _by_:
            _by_.requires(s)
        await self._tg.spawn(_service, s, proc, args, kwargs)
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
        s = Scope(self, self._main_name, new=True)
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

    async def __aenter__(self):
        if self._ctx_ is not None:
            raise RuntimeError("A ScopeSet can only be used once")
        self._ctx_ = self._ctx()
        return await self._ctx_.__aenter__()

    async def __aexit__(self, *tb):
        return await self._ctx_.__aexit__(*tb)  # pylint:disable=no-member  # YES IT HAS

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self._main_name)


async def spawn_service(proc, *args, **kwargs):
    """
    Run 'proc' in a new service scope, i.e. one that should end *after* the
    current scope terminates.

    Special arguments:
        _name_: Name for the new scope

    Returns: the new scope.
    """
    return await scope.get().spawn_service(proc, *args, **kwargs)


async def spawn(proc, *args, **kwargs):
    """
    Run 'proc' as a subtask of the current scope.

    Returns: a cancel scope, useable to cancel this subtask.
    """
    return await scope.get().spawn(proc, *args, **kwargs)


async def service(name, proc, *args, **kwargs):
    """
    Start this service as a dependent context if it doesn't run
    already.

    Returns: the data which the service registers / has registered.
    """
    return await scope.get().service(name, proc, *args, **kwargs)


def lookup(name):
    """
    Return the data associated with some scope.

    Raises KeyError if the named scope doesn't exist or has not
    provided any data.

    Use this if you need a temporary reference to the data of a scope which
    might depend on the current one.
    """
    return scope.get().lookup(name)


def lookup_scope(name):
    """
    Return the scope associated with some name.

    Raises KeyError if the named scope doesn't exist.

    Use this if you need to add a dependency.
    """
    return scope.get().lookup(name)


async def register(data: Any):
    """
    Register some data with this scope.

    The data is returned by calls to `service` from other scopes.
    """
    return await scope.get().register(data)


def requires(s: Scope):
    """
    This scope requires another (already existing) scope in order to function.
    """
    return scope.get().requires(s)


async def no_more_dependents():
    """
    Wait until all of your dependents are gone.

    Call this if you want to clean up in a controlled way instead of
    getting cancelled.

    You *must* terminate your scope if this returns!
    """
    return await scope.get().no_more_dependents()


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
            await s.cancel_dependents()  # should not be any but …
            await s.cancel()
    pass  # end main scope
