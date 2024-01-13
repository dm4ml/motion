"""
This file tests the generator and async generator functionality.
"""
import pytest
from motion import Component
import random

C = Component("GeneratorComponent")


@C.init_state
def setUp():
    return {}


@C.serve("identity")
def identity(state, props):
    k = props["k"]

    # Randomly generate a list of k values
    values = [random.randint(0, 100) for _ in range(k)]

    # Return an iterator over the props values
    for v in values:
        yield v


@C.update("identity")
def assert_list(state, props):
    # Add serve result to state
    return {"sync_serve_result": props.serve_result}


@C.serve("async_identity")
async def async_identity(state, props):
    k = props["k"]

    # Randomly generate a list of k values
    values = [random.randint(0, 100) for _ in range(k)]

    # Return an iterator over the props values
    for v in values:
        yield v


@C.update("async_identity")
async def async_assert_list(state, props):
    # Add serve result to state
    return {"async_serve_result": props.serve_result}


def test_regular_generator():
    c = C("some_instance")

    serve_values = []
    for v in c.gen("identity", props={"k": 3}, flush_update=True):
        serve_values.append(v)
    assert len(serve_values) == 3

    assert c.read_state("sync_serve_result") == serve_values
    c.shutdown()

    # Open the instance again and read the cached result
    c = C("some_instance")
    assert c.read_state("sync_serve_result") == serve_values

    new_serve_values = []
    for v in c.gen("identity", props={"k": 3}):
        new_serve_values.append(v)
    assert new_serve_values == serve_values, "Cached result should be returned"

    c.shutdown()


@pytest.mark.asyncio
async def test_async_generator():
    c = C("some_async_instance")

    serve_values = []
    async for v in c.agen("async_identity", props={"k": 3}, flush_update=True):
        serve_values.append(v)
    assert len(serve_values) == 3

    assert c.read_state("async_serve_result") == serve_values
    c.shutdown()

    # Open the instance again and read the cached result
    c = C("some_async_instance")
    assert c.read_state("async_serve_result") == serve_values

    new_serve_values = []
    async for v in c.agen("async_identity", props={"k": 3}):
        new_serve_values.append(v)
    assert new_serve_values == serve_values, "Cached result should be returned"

    c.shutdown()
