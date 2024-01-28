"""
This file tests the functionality to set expiration policies for update queues.
We will test two expiration policies: NUM_NEW_UPDATES and SECONDS.
We will test them in both sync and async functions.
"""
from motion import Component, ExpirePolicy
import time
import asyncio
import pytest

C = Component("C")


@C.init_state
def setup():
    return {"num_new_update_value": 0, "regular_value": 0, "seconds_value": 0}


@C.update("sum", expire_after=10, expire_policy=ExpirePolicy.NUM_NEW_UPDATES)
def update_sum_num_new(state, props):
    # Sleep for a bit so there's a real bottleneck in the update queue
    time.sleep(0.02)
    return {"num_new_update_value": state["num_new_update_value"] + props["value"]}


@C.update("sum")
def update_sum_default(state, props):
    return {"regular_value": state["regular_value"] + props["value"]}


@C.update("sum", expire_after=1, expire_policy=ExpirePolicy.SECONDS)
def update_sum_seconds(state, props):
    time.sleep(0.05)
    return {"seconds_value": state["seconds_value"] + props["value"]}


@C.update("asum", expire_after=10, expire_policy=ExpirePolicy.NUM_NEW_UPDATES)
async def aupdate_sum_num_new(state, props):
    # Sleep for a bit so there's a real bottleneck in the update queue
    await asyncio.sleep(0.01)
    return {"num_new_update_value": state["num_new_update_value"] + props["value"]}


@C.update("asum")
async def aupdate_sum_default(state, props):
    return {"regular_value": state["regular_value"] + props["value"]}


@C.update("asum", expire_after=1, expire_policy=ExpirePolicy.SECONDS)
async def aupdate_sum_seconds(state, props):
    await asyncio.sleep(0.05)
    return {"seconds_value": state["seconds_value"] + props["value"]}


def test_sync_num_new_updates():
    c = C()

    for i in range(50):
        c.run("sum", props={"value": i})

    # Flush instance
    c.flush_update("sum")

    # Assert new state is different from old state
    assert c.get_version() > 1
    assert c.read_state("num_new_update_value") != 0
    assert c.read_state("seconds_value") != 0

    # Assert that the num_new_update_value is not sum of 1..50 because there was the expiration policy
    assert c.read_state("num_new_update_value") != sum(range(50))
    assert c.read_state("seconds_value") != sum(range(50))

    # Assert that regular_value is sum of 1..50 because there was no expiration policy
    assert c.read_state("regular_value") == sum(range(50))

    c.shutdown()


@pytest.mark.asyncio
async def test_async_num_new_updates():
    c = C()

    async_tasks = []
    for i in range(50):
        async_tasks.append(c.arun("asum", props={"value": i}))

    await asyncio.gather(*async_tasks)

    # Flush instance
    c.flush_update("asum")

    # Assert new state is different from old state
    assert c.get_version() > 1
    assert c.read_state("num_new_update_value") != 0
    assert c.read_state("seconds_value") != 0

    # Assert that the num_new_update_value is not sum of 1..50 because there was the expiration policy
    assert c.read_state("num_new_update_value") != sum(range(50))
    assert c.read_state("seconds_value") != sum(range(50))

    # Assert that regular_value is sum of 1..50 because there was no expiration policy
    assert c.read_state("regular_value") == sum(range(50))

    c.shutdown()
