from motion import Component
import pytest

Calculator = Component("Calculator")


@Calculator.init_state
def setUp():
    return {"value": 0}


@Calculator.infer("add")
def plus(state, value):
    return state["value"] + value


@Calculator.fit("add")
def increment(state, value, infer_result):
    return {"value": state["value"] + value}


@Calculator.infer("subtract")
def minus(state, value):
    return state["value"] - value


@Calculator.fit("subtract")
def decrement(state, value, infer_result):
    return {"value": state["value"] - value}


@Calculator.infer("identity")
def noop(state, value):
    return value


@Calculator.fit("reset")
def reset(state, infer_result):
    return {"value": 0}


def test_multiple_routes():
    c = Calculator()
    assert c.run("add", kwargs={"value": 1}, flush_fit=True) == 1
    assert c.run("add", kwargs={"value": 2}, flush_fit=True) == 3
    assert c.run("subtract", kwargs={"value": 1}, flush_fit=True) == 2
    assert c.run("identity", kwargs={"value": 1}) == 1

    # Force fit doesn't do anything
    c.run("identity", kwargs={"value": 1}, flush_fit=True)

    c.run("reset", flush_fit=True)
    assert c.run("add", kwargs={"value": 1}, flush_fit=True) == 1
