from motion import Component
import pytest


def test_create():
    c = Component("Counter")

    @c.init_state
    def setUp():
        return {"value": 0}

    @c.infer("number")
    def noop(state, value):
        return state["value"], value

    @c.fit("number", batch_size=1)
    def increment(state, values, infer_results):
        return {"value": state["value"] + sum(values)}

    with pytest.raises(ValueError):
        # Read state before run
        c.read_state("value")

    assert c.run(number=1)[1] == 1
    _, fit_event = c.run(number=2, return_fit_event=True)
    fit_event.wait()
    assert c.run(number=3, wait_for_fit=True)[1] == 3
    assert c.run(number=4, wait_for_fit=True)[0] == 6

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
