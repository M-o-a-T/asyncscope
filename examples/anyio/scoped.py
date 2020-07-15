
import anyio
from asyncscope import spawn, spawn_service, run, Scope, scope, main_scope
from contextlib import asynccontextmanager

_done = None

await self.master.send(

sub_scope=False
with_error = False

@asynccontextmanager
async def maybe(p,*x):
    if sub_scope:
        async with p(*x) as s:
            yield s
    else:
        yield "foo"

async def dly():
    async with anyio.open_cancel_scope(shield=True):
        await anyio.sleep(0.1)

async def serv_c():
    print("serv_c: startup")
    try:
        print("serv_c: sleep")
        await anyio.sleep(1)
        if with_error:
            raise RuntimeError("Bye")
        print("serv_c: set _done")
        await _done.set()
        await anyio.sleep(999)
    finally:
        await dly()
        print("serv_c: end")

async def serv_b():
    print("serv_b: startup")
    try:
        await spawn_service(serv_c)
        print("serv_b: sleep")
        await anyio.sleep(999)
    finally:
        await dly()
        print("serv_b: end")

async def main_a():
    print("main_a: startup")
    try:
        async with maybe(main_scope,"test"):
            await spawn_service(serv_b)
            print("main_a: sleep")
            await anyio.sleep(999)
    finally:
        await dly()
        print("main_a: end")

async def main():
    print("main: startup")
    global _done
    _done = anyio.create_event()

    await spawn(main_a)

    print("main: waiting")
    await _done.wait()

    print("main: ending")


run(main)
