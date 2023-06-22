from motion import Component
import pytest

Counter = Component("Counter")


@Counter.init_state
def setUp():
    return {"value": 0}


@Counter.infer("number")
def noop(state, value):
    return state["value"], value


@Counter.fit("number")
def increment(state, value, infer_result):
    return {"value": state["value"] + value}


def test_create():
    c = Counter()

    assert c.read_state("value") == 0

    assert c.run("number", kwargs={"value": 1})[1] == 1
    c.run("number", kwargs={"value": 2}, flush_fit=True)
    assert c.run("number", kwargs={"value": 3}, flush_fit=True)[1] == 3
    assert c.run("number", kwargs={"value": 4}, flush_fit=True)[0] == 6

    # Should raise errors
    with pytest.raises(KeyError):
        c.run(6)

    # Get value
    assert c.read_state("value") == 10
    with pytest.raises(KeyError):
        c.read_state("DNE")


def test_fit_error():
    c = Counter()

    # Should raise error bc fit op won't work
    with pytest.raises(RuntimeError):
        c.run("number", kwargs={"value": [1]}, flush_fit=True)
