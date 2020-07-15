
import trio
from trio_scope import spawn, spawn_service, run, Scope, scope, main_scope

_done = None

async def dly():
    with trio.CancelScope() as sc:
        sc.shield = True
        await trio.sleep(0.1)

async def serv_c():
    print("serv_c: startup")
    try:
        print("serv_c: sleep")
        await trio.sleep(1)
        if False:
            raise RuntimeError("Bye")
        print("serv_c: set _done")
        _done.set()
        await trio.sleep(999)
    finally:
        await dly()
        print("serv_c: end")

async def serv_b():
    print("serv_b: startup")
    try:
        await spawn_service(serv_c)
        print("serv_b: sleep")
        await trio.sleep(999)
    finally:
        await dly()
        print("serv_b: end")

async def main_a():
    print("main_a: startup")
    try:
        async with main_scope("test"):
            assert scope.get()._name == "test"
            await spawn_service(serv_b)
            print("main_a: sleep")
            await trio.sleep(999)
    finally:
        await dly()
        print("main_a: end")

async def main():
    print("main: startup")
    global _done
    _done = trio.Event()

    await spawn(main_a)

    print("main: waiting")
    await _done.wait()

    print("main: ending")


run(main)
