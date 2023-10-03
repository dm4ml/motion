from motion import Component
import pytest

Counter = Component("Counter")


@Counter.init_state
def setUp():
    return {"value": 0}


@Counter.serve("number")
def noop(state, props):
    return state["value"], props["value"]


@Counter.update("number")
def increment(state, props):
    return {"value": state["value"] + props["value"]}


def test_create():
    c = Counter(disable_update_task=True)

    assert c.read_state("value") == 0

    assert c.run("number", props={"value": 1}, flush_update=True)[1] == 1
    c.run("number", props={"value": 2}, flush_update=True)
    assert c.run("number", props={"value": 3}, flush_update=True)[1] == 3
    assert c.run("number", props={"value": 4}, flush_update=True)[0] == 6

    # Should raise errors
    with pytest.raises(KeyError):
        c.run(6)

    # Get value
    assert c.read_state("value") == 10
    assert c.read_state("DNE") is None


def test_fit_error():
    c = Counter()

    # Should raise error bc update op won't work
    with pytest.raises(RuntimeError):
        c.run("number", props={"value": [1]}, flush_update=True)
