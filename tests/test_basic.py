import logging
from random import random

import anyio
import pytest

from asyncscope import main_scope, scope, ScopeDied

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
    async with main_scope():
        stp = Stepper()
        await scope.spawn(main_a, stp)
        await _done.wait()
    # Leaving the ScopeSet triggers a controlled cancellation
    stp(4)


@pytest.mark.anyio
async def test_main_error_a():
    async def serv_c(stp):
        try:
            scope.register(S("C"))
            await anyio.sleep(0.1)
            _done.set()
            await anyio.sleep(9999)
        finally:
            await dly()
            stp(3)

    async def serv_b(stp):
        try:
            await scope.spawn_service(serv_c, stp)
            scope.register(S("B"))
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
        async with main_scope():
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
    async with main_scope():
        evt = anyio.Event()
        await scope.spawn(main_a, evt)
        await evt.wait()
        await _done.wait()
    # Leaving the ScopeSet triggers a controlled cancellation
    assert _steps == 11222211


@pytest.mark.anyio
@pytest.mark.parametrize("err",[False,True])
async def test_using(err):
    evt = anyio.Event()
    evt2 = anyio.Event()

    async def srv_c():
        logger.debug("C pre")
        scope.register(42)
        logger.debug("C waiting")
        await scope.no_more_dependents()
        logger.debug("C post")

    async def srv_b():
        logger.debug("B pre")
        c = await scope.service("C", srv_c)
        assert c == 42
        scope.register(123)
        logger.debug("B run")
        async with anyio.create_task_group() as tg:
            @tg.start_soon
            async def evw():
                await evt.wait()
                logger.debug("B ending")
                tg.cancel_scope.cancel()

            @tg.start_soon
            async def dpw():
                await scope.wait_no_users()
                logger.debug("B no more users")
                tg.cancel_scope.cancel()
        logger.debug("B post")

    async def srv_a():
        logger.debug("A pre")
        scope.register(69)
        s = scope.get()
        async with scope.using_scope():
            b = await scope.service("B",srv_b)
            assert b == 123
            logger.debug("A run")
            await s.wait_no_users()
            logger.debug("A releasing B")
        logger.debug("A post")

    async def srv_aa():
        logger.debug("AA pre")
        s = scope.get()

        with pytest.raises(ScopeDied):
            async with scope.using_scope():
                b = await scope.service("B",srv_b)
                assert b == 123
                s.register(69)
                logger.debug("AA run")
                await s.wait_no_users()
                raise RuntimeError("should not get here")
            raise RuntimeError("should not get here either")

        logger.debug("AA cont")
        evt2.set()
        await s.wait_no_users()
        if err:
            nonlocal x
            x = True
        logger.debug("AA end")

    x = False
    try:
        async with main_scope():
            logger.debug("Main pre")
            a = await scope.service("A", srv_aa if err else srv_a, _as_scope=True)
            assert a.data == 69
            await anyio.sleep(0.1)
            logger.debug("Main run")
            await anyio.sleep(0.1)
            if err:
                logger.debug("Main trigger error")
                evt.set()
                await evt2.wait()
                await anyio.sleep(0.2)
            logger.debug("Main releasing A")
            scope.release(a)
            logger.debug("Main released A")
            await anyio.sleep(0.1)
            logger.debug("Main ends")
            if not err:
                x = True
        logger.debug("Main ended")
    finally:
        assert x
