import logging
from random import random

import anyio
import pytest

from asyncscope import ScopeSet, scope

from . import Stepper

logger = logging.getLogger(__name__)
logger.root.level = logging.DEBUG
logger.level = logging.DEBUG

_done = None


async def dly():
    with anyio.CancelScope(shield=True):
        await anyio.sleep(0.02 + random() * 0.05)


@pytest.mark.anyio
async def test_main():
    async def serv_c(stp):
        try:
            await anyio.sleep(0.1)
            _done.set()
            await anyio.sleep(999)
        finally:
            await dly()
            stp(3)

    async def serv_b(stp):
        try:
            await scope.spawn_service(serv_c, stp)
            await anyio.sleep(999)
        finally:
            await dly()
            stp(2)

    async def main_a(stp):
        try:
            await scope.spawn_service(serv_b, stp)
            await anyio.sleep(999)
        finally:
            await dly()
            stp(1)

    global _done
    _done = anyio.Event()
    async with ScopeSet():
        stp = Stepper()
        await scope.spawn(main_a, stp)
        await _done.wait()
    # Leaving the ScopeSet triggers a controlled cancellation
    stp(4)


@pytest.mark.anyio
async def test_main_error_a():
    async def serv_c(stp):
        try:
            await anyio.sleep(0.1)
            _done.set()
            await anyio.sleep(9999)
        finally:
            await dly()
            stp(3)

    async def serv_b(stp):
        try:
            await scope.spawn_service(serv_c, stp)
            await anyio.sleep(9999)
        finally:
            await dly()
            stp(2)

    async def main_a(stp):
        try:
            await scope.spawn_service(serv_b, stp)
            await _done.wait()
            await anyio.sleep(0.3)
            raise RuntimeError("Bye")
        finally:
            await dly()
            stp(1)

    global _done
    _done = anyio.Event()
    with pytest.raises(RuntimeError) as e:
        async with ScopeSet():
            stp = Stepper()
            await scope.spawn(main_a, stp)
            await _done.wait()
            await anyio.sleep(9999)
    assert e.value.args == ("Bye",)


class S(str):
    "A string we can set attributes on"
    pass


@pytest.mark.anyio
async def test_diamond():
    _steps = 0

    def steps(n):
        nonlocal _steps
        _steps += n

    async def serv_d():
        try:
            steps(1)
            await anyio.sleep(0.3)
            scope.register(S("D"))
            _done.set()
            await anyio.sleep(999)
        finally:
            await dly()
            steps(10)

    async def serv_c():
        try:
            steps(100)
            scope.register(S("C"))
            await scope.service("D", serv_d)
            await anyio.sleep(999)
        finally:
            await dly()
            steps(1000)

    async def serv_b():
        try:
            steps(10000)
            c1 = await scope.service("C2", serv_c)
            c2 = await scope.service("C1", serv_c)
            scope.register(S("B"))
            assert c1 == "C"
            assert c2 == "C"
            with anyio.fail_after(9):
                await scope.no_more_dependents()
            steps(100_000)
        finally:
            await dly()

    async def main_a(evt):
        try:
            steps(1_000_000)
            b1 = await scope.service("B1", serv_b)
            b2 = await scope.service("B2", serv_b)
            evt.set()
            assert b1 == "B"
            assert b2 == "B"

            await anyio.sleep(999)
        finally:
            steps(10_000_000)

    global _done
    _done = anyio.Event()
    async with ScopeSet():
        evt = anyio.Event()
        await scope.spawn(main_a, evt)
        await evt.wait()
        await _done.wait()
    # Leaving the ScopeSet triggers a controlled cancellation
    assert _steps == 11222211
