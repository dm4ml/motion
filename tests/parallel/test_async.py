from motion import Component

import asyncio
import pytest

Counter = Component("Counter")


@Counter.init_state
def setup():
    return {"value": 1}


@Counter.serve("multiply")
async def noop(state, props):
    await asyncio.sleep(0.01)
    return state["value"] * props["value"]


@Counter.serve("sync_multiply")
def sync_noop(state, props):
    return state["value"] * props["value"]


@Counter.update("multiply")
async def increment(state, props):
    return {"value": state["value"] + 1}


@pytest.mark.asyncio
async def test_async_serve():
    c = Counter()
    assert await c.arun("multiply", props={"value": 2}) == 2

    # Test that the user can't call run
    with pytest.raises(TypeError):
        c.run("multiply", props={"value": 2}, force_refresh=True)

    # Test that the user can call arun for regular functions
    result = await c.arun("sync_multiply", props={"value": 2})
    assert result == 4


@pytest.mark.asyncio
async def test_async_update():
    c = Counter(disable_update_task=True)

    await c.arun("multiply", props={"value": 2}, flush_update=True)
    assert c.read_state("value") == 2


@pytest.mark.asyncio
@pytest.mark.timeout(1)  # This test should take less than 3 seconds
async def test_gather():
    c = Counter(disable_update_task=True)

    tasks = [
        c.arun("multiply", props={"value": i}, flush_update=True) for i in range(100)
    ]
    # Run all tasks at the same time
    await asyncio.gather(*tasks)

    # Assert new state
    assert c.read_state("value") == 101
