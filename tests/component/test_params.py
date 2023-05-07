from motion import Component

import pytest


def test_params():
    c = Component("ComponentWithParams", params={"multiplier": 2})

    @c.setUp
    def setUp():
        return {"value": 0}

    @c.infer("add")
    def plus(state, value):
        return c.params["multiplier"] * (state["value"] + value)

    @c.fit("add", batch_size=1)
    def increment(state, values, infer_results):
        return {"value": state["value"] + sum(values)}

    assert c.run(add=1, wait_for_fit=True) == 2
    assert c.run(add=2, wait_for_fit=True) == 6


def test_params_not_exist():
    c = Component("ComponentWithParams")

    @c.setUp
    def setUp():
        return {"value": 0}

    @c.infer("add")
    def plus(state, value):
        return c.params["multiplier"] * (state["value"] + value)

    @c.fit("add", batch_size=1)
    def increment(state, values, infer_results):
        return {"value": state["value"] + sum(values)}

    with pytest.raises(KeyError):
        assert c.run(add=1, wait_for_fit=True) == 2
