"""
This file is used to test functionality of running a dataflow for many keys.
"""
from motion import Component

import time

Counter = Component("Counter")


@Counter.init_state
def setup():
    return {"value": 1, "multifit": []}


@Counter.infer(["increment", "decrement"])
def noop(state, value):
    return value


@Counter.fit("increment")
def increment(state, value, infer_result):
    return {"value": state["value"] + 1}


@Counter.fit("decrement")
def nothing(state, value, infer_result):
    return {"value": state["value"] - 1}


@Counter.fit(["accumulate", "something_else"])
def multifit(state, value, infer_result):
    return {"multifit": state["multifit"] + [value]}


def test_many_keys():
    c = Counter()

    c.run("increment", kwargs={"value": 1}, flush_fit=True)
    assert c.read_state("value") == 2
    c.run("decrement", kwargs={"value": 1}, flush_fit=True)
    assert c.read_state("value") == 1

    # Test multifit
    c.run("accumulate", kwargs={"value": 1})
    c.run("something_else", kwargs={"value": 2})

    c.flush_fit("accumulate")
    c.flush_fit("something_else")

    assert c.read_state("multifit") == [1, 2]
