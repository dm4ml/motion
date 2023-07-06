from motion import Component

import asyncio
import pytest

Counter = Component("Counter")


@Counter.init_state
def setup():
    return {"value": 1}


@Counter.serve("multiply")
async def noop(state, value):
    await asyncio.sleep(0.5)
    return state["value"] * value


@Counter.serve("sync_multiply")
def sync_noop(state, value):
    return state["value"] * value


@Counter.update("multiply")
async def increment(state, value, serve_result):
    return {"value": state["value"] + 1}


@pytest.mark.asyncio
async def test_async_serve():
    c = Counter()
    assert await c.arun("multiply", kwargs={"value": 2}) == 2

    # Test that the user can't call run
    with pytest.raises(TypeError):
        c.run("multiply", kwargs={"value": 2}, force_refresh=True)

    # Test that the user can't call arun for regular functions
    with pytest.raises(TypeError):
        assert await c.arun("sync_multiply", kwargs={"value": 2}) == 2


@pytest.mark.asyncio
async def test_async_update():
    c = Counter()

    await c.arun("multiply", kwargs={"value": 2}, flush_update=True)
    assert c.read_state("value") == 2


@pytest.mark.asyncio
@pytest.mark.timeout(3)  # This test should take less than 3 seconds
async def test_gather():
    c = Counter()

    tasks = [
        c.arun("multiply", kwargs={"value": i}, force_refresh=True)
        for i in range(100)
    ]
    # Run all tasks at the same time
    await asyncio.gather(*tasks)

    # Flush instance
    c.flush_update("multiply")

    # Assert new state
    assert c.read_state("value") == 101
