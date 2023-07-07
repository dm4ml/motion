from motion import Component
import pytest

Calculator = Component("Calculator")


@Calculator.init_state
def setUp():
    return {"value": 0}


@Calculator.serve("add")
def plus(state, props):
    return state["value"] + props["value"]


@Calculator.serve("subtract")
def minus(state, props):
    return state["value"] - props["value"]


@Calculator.update(["add", "subtract"])
def decrement(state, props):
    return {"value": props.serve_result}


@Calculator.serve("identity")
def noop(state, props):
    return props["value"]


@Calculator.update("reset")
def reset(state, props):
    return {"value": 0}


def test_multiple_routes():
    c = Calculator()
    assert c.run("add", props={"value": 1}, flush_update=True) == 1
    assert c.run("add", props={"value": 2}, flush_update=True) == 3
    assert c.run("subtract", props={"value": 1}, flush_update=True) == 2
    assert c.run("identity", props={"value": 1}) == 1

    # Force update doesn't do anything
    c.run("identity", props={"value": 1}, flush_update=True)

    c.run("reset", flush_update=True)
    assert c.run("add", props={"value": 1}, flush_update=True) == 1
