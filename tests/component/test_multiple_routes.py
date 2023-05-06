from motion import Component
import pytest


class Calculator(Component):
    def setUp(self):
        return {"value": 0}

    @Component.infer("add")
    def plus(self, state, value):
        return state["value"] + value

    @Component.fit("add", batch_size=1)
    def increment(self, state, values, infer_results):
        return {"value": state["value"] + sum(values)}

    @Component.infer("subtract")
    def minus(self, state, value):
        return state["value"] - value

    @Component.fit("subtract", batch_size=1)
    def decrement(self, state, values, infer_results):
        return {"value": state["value"] - sum(values)}

    @Component.infer("identity")
    def noop(self, state, value):
        return value

    @Component.fit("reset", batch_size=1)
    def reset(self, state, values, infer_results):
        return {"value": 0}


def test_multiple_routes():
    c = Calculator()
    assert c.run(add=1, wait_for_fit=True) == 1
    assert c.run(add=2, wait_for_fit=True) == 3
    assert c.run(subtract=1, wait_for_fit=True) == 2
    assert c.run(identity=1) == 1

    with pytest.raises(ValueError):
        c.run(identity=1, wait_for_fit=True)

    c.run(reset=1, wait_for_fit=True)
    assert c.run(add=1, wait_for_fit=True) == 1
