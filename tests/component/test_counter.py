from motion import Component
import pytest

Counter = Component("Counter")


@Counter.init_state
def setUp():
    return {"value": 0}


@Counter.infer("number")
def noop(state, value):
    return state["value"], value


@Counter.fit("number", batch_size=1)
def increment(state, values, infer_results):
    return {"value": state["value"] + sum(values)}


def test_create():
    c = Counter()

    assert c.read_state("value") == 0

    assert c.run(number=1)[1] == 1
    c.run(number=2, flush_fit=True)
    assert c.run(number=3, flush_fit=True)[1] == 3
    assert c.run(number=4, flush_fit=True)[0] == 6

    # Should raise errors
    with pytest.raises(KeyError):
        c.run(number2=6)

    with pytest.raises(ValueError):
        c.run(number=6, number2=7)

    with pytest.raises(ValueError):
        c.run()

    with pytest.raises(TypeError):
        c.run(6)

    # Get value
    assert c.read_state("value") == 10
    with pytest.raises(KeyError):
        c.read_state("DNE")
