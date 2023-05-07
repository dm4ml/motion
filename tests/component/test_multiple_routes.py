from motion import Component
import pytest


def test_multiple_routes():
    c = Component("Calculator")

    @c.setUp
    def setUp():
        return {"value": 0}

    @c.infer("add")
    def plus(state, value):
        return state["value"] + value

    @c.fit("add", batch_size=1)
    def increment(state, values, infer_results):
        return {"value": state["value"] + sum(values)}

    @c.infer("subtract")
    def minus(state, value):
        return state["value"] - value

    @c.fit("subtract", batch_size=1)
    def decrement(state, values, infer_results):
        return {"value": state["value"] - sum(values)}

    @c.infer("identity")
    def noop(state, value):
        return value

    @c.fit("reset", batch_size=1)
    def reset(state, values, infer_results):
        return {"value": 0}

    assert c.run(add=1, wait_for_fit=True) == 1
    assert c.run(add=2, wait_for_fit=True) == 3
    assert c.run(subtract=1, wait_for_fit=True) == 2
    assert c.run(identity=1) == 1

    with pytest.raises(ValueError):
        c.run(identity=1, wait_for_fit=True)

    c.run(reset=1, wait_for_fit=True)
    assert c.run(add=1, wait_for_fit=True) == 1
