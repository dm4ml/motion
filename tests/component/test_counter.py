from motion import Component
import pytest

Counter = Component("Counter")


@Counter.init_state
def setUp():
    return {"value": 0}


@Counter.serve("number")
def noop(state, value):
    return state["value"], value


@Counter.update("number")
def increment(state, value, serve_result):
    return {"value": state["value"] + value}


def test_create():
    c = Counter()

    assert c.read_state("value") == 0

    assert c.run("number", kwargs={"value": 1})[1] == 1
    c.run("number", kwargs={"value": 2}, flush_update=True)
    assert c.run("number", kwargs={"value": 3}, flush_update=True)[1] == 3
    assert c.run("number", kwargs={"value": 4}, flush_update=True)[0] == 6

    # Should raise errors
    with pytest.raises(KeyError):
        c.run(6)

    # Get value
    assert c.read_state("value") == 10
    with pytest.raises(KeyError):
        c.read_state("DNE")


def test_fit_error():
    c = Counter()

    # Should raise error bc update op won't work
    with pytest.raises(RuntimeError):
        c.run("number", kwargs={"value": [1]}, flush_update=True)
