from motion import Component

import pytest

Counter = Component("Counter")


@Counter.init_state
def setup():
    return {"value": 1}


@Counter.infer("multiply")
async def noop(state, value):
    return state["value"] * value


@Counter.fit("multiply", batch_size=1)
async def increment(state, values, infer_results):
    return {"value": state["value"] + 1}


@pytest.mark.asyncio
async def test_async_infer():
    c = Counter()
    assert await c.arun(multiply=2) == 2

    # Test that the user can't call run
    with pytest.raises(TypeError):
        c.run(multiply=2, force_refresh=True)


@pytest.mark.asyncio
async def test_async_fit():
    c = Counter()

    await c.arun(multiply=2, flush_fit=True)

    assert c.read_state("value") == 2
