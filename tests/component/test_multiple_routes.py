from motion import Component
import pytest

Calculator = Component("Calculator")


@Calculator.init_state
def setUp():
    return {"value": 0}


@Calculator.serve("add")
def plus(state, value):
    return state["value"] + value


@Calculator.update("add")
def increment(state, value, serve_result):
    return {"value": state["value"] + value}


@Calculator.serve("subtract")
def minus(state, value):
    return state["value"] - value


@Calculator.update("subtract")
def decrement(state, value, serve_result):
    return {"value": state["value"] - value}


@Calculator.serve("identity")
def noop(state, value):
    return value


@Calculator.update("reset")
def reset(state, serve_result):
    return {"value": 0}


def test_multiple_routes():
    c = Calculator()
    assert c.run("add", kwargs={"value": 1}, flush_update=True) == 1
    assert c.run("add", kwargs={"value": 2}, flush_update=True) == 3
    assert c.run("subtract", kwargs={"value": 1}, flush_update=True) == 2
    assert c.run("identity", kwargs={"value": 1}) == 1

    # Force update doesn't do anything
    c.run("identity", kwargs={"value": 1}, flush_update=True)

    c.run("reset", flush_update=True)
    assert c.run("add", kwargs={"value": 1}, flush_update=True) == 1
