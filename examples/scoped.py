
import anyio
from asyncscope import scope, main_scope
from contextlib import asynccontextmanager

_done = None

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
    with anyio.CancelScope(shield=True):
        await anyio.sleep(0.02)

async def serv_c():
    print("serv_c: startup")
    try:
        print("serv_c: sleep")
        await anyio.sleep(0.1)
        if with_error:
            raise RuntimeError("Bye")
        print("serv_c: set _done")
        _done.set()
        await anyio.sleep(999)
    finally:
        await dly()
        print("serv_c: end")

async def serv_b():
    print("serv_b: startup")
    try:
        await scope.spawn_service(serv_c)
        print("serv_b: sleep")
        await anyio.sleep(999)
    finally:
        await dly()
        print("serv_b: end")

async def main_a():
    print("main_a: startup")
    try:
        async with maybe(main_scope):
            await scope.spawn_service(serv_b)
            print("main_a: sleep")
            await anyio.sleep(999)
    finally:
        await dly()
        print("main_a: end")

async def main():
    print("main: startup")
    global _done
    _done = anyio.Event()

    async with main_scope():
        await scope.spawn(main_a)

        print("main: waiting")
        await _done.wait()

    print("main: ending")


anyio.run(main)
