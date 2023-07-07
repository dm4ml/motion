"""
This file is used to test functionality of running a dataflow for many keys.
"""
from motion import Component

import time

Counter = Component("Counter")


@Counter.init_state
def setup():
    return {"value": 1, "multifit": []}


@Counter.serve(["increment", "decrement"])
def noop(state, props):
    return props["value"]


@Counter.update("increment")
def increment(state, props):
    return {"value": state["value"] + 1}


@Counter.update("decrement")
def nothing(state, props):
    return {"value": state["value"] - 1}


@Counter.update(["accumulate", "something_else"])
def multiupdate(state, props):
    return {"multifit": state["multifit"] + [props["value"]]}


def test_many_keys():
    c = Counter()

    c.run("increment", props={"value": 1}, flush_update=True)
    assert c.read_state("value") == 2
    c.run("decrement", props={"value": 1}, flush_update=True)
    assert c.read_state("value") == 1

    # Test multifit
    c.run("accumulate", props={"value": 1})
    c.run("something_else", props={"value": 2})

    c.flush_update("accumulate")
    c.flush_update("something_else")

    assert c.read_state("multifit") == [1, 2]
