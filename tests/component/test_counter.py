from motion import Component
import pytest


class Counter(Component):
    def setUp(self):
        return {"value": 0}

    @Component.infer("number")
    def noop(self, state, value):
        return state["value"], value

    @Component.fit("number", batch_size=1)
    def increment(self, state, values, infer_results):
        return {"value": state["value"] + sum(values)}


def test_create():
    c = Counter()
    assert c.run(number=1)[1] == 1
    _, fit_event = c.run(number=2, return_fit_event=True)
    fit_event.wait()
    assert c.run(number=3, wait_for_fit=True)[1] == 3
    assert c.run(number=4)[0] == 6

    # Should raise errors
    with pytest.raises(KeyError):
        c.run(number2=6)

    with pytest.raises(ValueError):
        c.run(number=6, number2=7)

    with pytest.raises(ValueError):
        c.run()

    with pytest.raises(TypeError):
        c.run(6)
