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


@Counter.fit("increment", batch_size=1)
def increment(state, values, infer_results):
    return {"value": state["value"] + 1}


@Counter.fit("decrement", batch_size=1)
def nothing(state, values, infer_results):
    return {"value": state["value"] - 1}


@Counter.fit(["accumulate", "something_else"], batch_size=1)
def multifit(state, values, infer_results):
    return {"multifit": state["multifit"] + values}


def test_many_keys():
    c = Counter()

    c.run(increment=1, flush_fit=True)
    assert c.read_state("value") == 2
    c.run(decrement=1, flush_fit=True)
    assert c.read_state("value") == 1

    # Test multifit
    c.run(accumulate=1)
    c.run(something_else=2)

    c.flush_fit("accumulate")
    c.flush_fit("something_else")

    assert c.read_state("multifit") == [1, 2]
