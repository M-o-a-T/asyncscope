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
from typing import Any

__all__ = ["run"]

scope = ContextVar("scope", default=None)

class Scope:
    _next: Scope = None
    _prev: Scope = None
    _done: anyio.abc.Event = None
    _scope  = None
    # This is the taskgroup that controls the jobs running in this scope
    _tg: anyio.abc.TaskGroup = None

    # This is the taskgroup which contains all linked scopes
    _up_tg: anyio.abc.TaskGroup = None
    _name: str = None
    _new: bool = False

    def __init__(self, tg:anyio.abc.TaskGroup, name:str, new:bool=False):
        self._up_tg = tg
        self._name = name
        self._done = anyio.create_event()
        self._new = new

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


    @asynccontextmanager
    async def _gen(self):
        if self._scope is not None:
            raise RuntimeError("You can't enter a scope twice")
        async with anyio.create_task_group() as tg:
            self._tg = tg
            os = scope.get()
            self._scope = scope.set(self)

            if not self._new:
                self._prev = None if os is None else os._prev
                if self._prev is not None:
                    self._prev._next = self
                if os is not None:
                    os._prev = self
                self._next = os
            try:
                yield self
            finally:
                scope.reset(self._scope)
                async with anyio.open_cancel_scope(shield=True):
                    if self._next:
                        await self._next.cancel()
                        await self._next.wait()
                    await self._tg.cancel_scope.cancel()
                    prev, self._prev = self._prev, None
                    await self._done.set()
                    if prev:
                        prev._next = None
                        await prev.cancel()
                    self._tg = None

    async def __aenter__(self):
        self._gen_ = self._gen()
        return await self._gen_.__aenter__()
    async def __aexit__(self, *tb):
        return await self._gen_.__aexit__(*tb)

    async def cancel(self):
        """
        Cancel this scope.

        This will first cancel the scope(s) depending on this one.
        """
        if self._next:
            await self._next.cancel()
        if self._next:
            await self._next.wait()
        if self._tg:
            await self._tg.cancel_scope.cancel()

    async def wait(self):
        """
        Wait until this scope has terminated.
        """
        await self._done.wait()


async def spawn_service(proc, *args, _name_:str=None, **kwargs):
    """
    Run 'proc' in a new service scope, i.e. one that should end *after* the
    current scope terminates.

    Special arguments:
        _name_: Name for the new scope

    Returns: the new scope.
    """
    s = None
    if _name_ is None:
        _name_ = proc.__name__
    async def _service(proc, args, kwargs):
        nonlocal s
        async with anyio.open_cancel_scope(shield=True) as sc:
            os = scope.get()
            s = Scope(os._up_tg, _name_)
            async with s:
                await proc(*args, **kwargs)

    await scope.get()._up_tg.spawn(_service, proc, args, kwargs)
    return s

async def spawn(proc, *args, **kwargs):
    """
    Run 'proc' as a subtask in the current scope.

    Returns: a cancel scope, useable to cancel this subtask.
    """
    return await scope.get().spawn(proc, *args, **kwargs)


@asynccontextmanager
async def main_scope(_name_="main"):
    """
    This context manager provides you with a new "main" scope, i.e. one you
    can start service tasks in.
    """
    async with anyio.create_task_group() as tg, Scope(tg,_name_,new=True) as s:
        try:
            yield s
        finally:
            await s.cancel()

async def _main(proc, args, kwargs):
    async with main_scope():
        return await proc(*args, **kwargs)

def run(proc, *args, **kwargs):
    return anyio.run(_main, proc, args, kwargs, backend="trio")
