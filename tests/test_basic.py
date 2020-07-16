import anyio
from asyncscope import (
    spawn,
    spawn_service,
    run,
    Scope,
    scope,
    main_scope,
    ScopeSet,
    register,
    service,
    no_more_dependents,
)
from . import Stepper
from contextlib import asynccontextmanager
import pytest
from random import random

_done = None


async def dly():
    async with anyio.open_cancel_scope(shield=True):
        await anyio.sleep(0.02 + random() * 0.05)


@pytest.mark.anyio
async def test_main():
    async def serv_c(stp):
        try:
            await anyio.sleep(0.1)
            await _done.set()
            await anyio.sleep(999)
        finally:
            await dly()
            stp(3)

    async def serv_b(stp):
        try:
            await spawn_service(serv_c, stp)
            await anyio.sleep(999)
        finally:
            await dly()
            stp(2)

    async def main_a(stp):
        try:
            await spawn_service(serv_b, stp)
            await anyio.sleep(999)
        finally:
            await dly()
            stp(1)

    global _done
    _done = anyio.create_event()
    async with ScopeSet():
        stp = Stepper()
        await spawn(main_a, stp)
        await _done.wait()
    # Leaving the ScopeSet triggers a controlled cancellation
    stp(4)


@pytest.mark.anyio
async def test_main_error_a():
    async def serv_c(stp):
        try:
            await anyio.sleep(0.1)
            raise RuntimeError("Bye")
        finally:
            await dly()
            stp(1)

    async def serv_b(stp):
        try:
            await spawn_service(serv_c, stp)
            await anyio.sleep(99)
        finally:
            await dly()
            stp(3)

    async def main_a(stp):
        try:
            await spawn_service(serv_b, stp)
            await anyio.sleep(99)
        finally:
            await dly()
            stp(2)

    global _done
    _done = anyio.create_event()
    with pytest.raises(RuntimeError) as e:
        async with ScopeSet():
            stp = Stepper()
            await spawn(main_a, stp)
            await _done.wait()
            await anyio.sleep(99)
    assert e.value.args == ("Bye",)


@pytest.mark.anyio
async def test_diamond():
    _steps = 0

    def steps(n):
        nonlocal _steps
        _steps += n

    async def serv_d():
        try:
            steps(1)
            await anyio.sleep(0.1)
            await register("D")
            await _done.set()
            await anyio.sleep(999)
        finally:
            await dly()
            steps(10)

    async def serv_c():
        try:
            steps(100)
            await service("D", serv_d)
            await register("C")
            await anyio.sleep(999)
        finally:
            await dly()
            steps(1000)

    async def serv_b():
        try:
            steps(10000)
            c1 = await service("C2", serv_c)
            c2 = await service("C1", serv_c)
            await register("B")
            assert c1 == "C"
            assert c2 == "C"
            async with anyio.fail_after(9):
                await no_more_dependents()
            steps(100_000)
        finally:
            await dly()

    async def main_a(evt):
        try:
            steps(1_000_000)
            b1 = await service("B1", serv_b)
            b2 = await service("B2", serv_b)
            await evt.set()
            assert b1 == "B"
            assert b2 == "B"

            await anyio.sleep(999)
        finally:
            steps(10_000_000)

    global _done
    _done = anyio.create_event()
    async with ScopeSet():
        evt = anyio.create_event()
        await spawn(main_a, evt)
        await evt.wait()
        await _done.wait()
    # Leaving the ScopeSet triggers a controlled cancellation
    assert _steps == 11222211
