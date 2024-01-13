"""
This file tests the generator and async generator functionality.
"""
import pytest
from motion import Component

C = Component("GeneratorComponent")


@C.init_state
def setUp():
    return {"value": 1}


@C.serve("identity")
def identity(state, props):
    values = props["values"]

    # Return an iterator over the props values
    for v in values:
        yield v


@C.update("identity")
def assert_list(state, props):
    # Add serve result to state
    return {"sync_serve_result": props.serve_result}


@C.serve("async_identity")
async def async_identity(state, props):
    values = props["values"]

    # Return an iterator over the props values
    for v in values:
        yield v


@C.update("async_identity")
async def async_assert_list(state, props):
    # Add serve result to state
    return {"async_serve_result": props.serve_result}


def test_regular_generator():
    c = C("some_instance")
    values = [1, 2, 3]

    serve_values = []
    for v in c.gen("identity", props={"values": values}, flush_update=True):
        serve_values.append(v)
    assert serve_values == values

    assert c.read_state("sync_serve_result") == values
    c.shutdown()

    # Open the instance again and read the cached result
    c = C("some_instance")
    assert c.read_state("sync_serve_result") == values

    serve_values = []
    for v in c.gen("identity", props={"values": values}):
        serve_values.append(v)
    assert serve_values == values, "Cached result should be returned"

    c.shutdown()


@pytest.mark.asyncio
async def test_async_generator():
    c = C("some_async_instance")
    values = [4, 5, 6]

    serve_values = []
    async for v in c.agen(
        "async_identity", props={"values": values}, flush_update=True
    ):
        serve_values.append(v)
    assert serve_values == values

    assert c.read_state("async_serve_result") == values
    c.shutdown()

    # Open the instance again and read the cached result
    c = C("some_async_instance")
    assert c.read_state("async_serve_result") == values

    serve_values = []
    async for v in c.agen("async_identity", props={"values": values}):
        serve_values.append(v)
    assert serve_values == values, "Cached result should be returned"

    c.shutdown()
