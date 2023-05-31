from motion import Component

import pytest

c = Component("ComponentWithParams", params={"multiplier": 2})


@c.init_state
def setUp():
    return {"value": 0}


@c.infer("add")
def plus(state, value):
    return c.params["multiplier"] * (state["value"] + value)


@c.fit("add", batch_size=1)
def increment(state, values, infer_results):
    return {"value": state["value"] + sum(values)}


def test_params():
    c_instance = c()
    assert c_instance.run(add=1, force_fit=True) == 2
    assert c_instance.run(add=2, force_fit=True) == 6


cwp = Component("ComponentWithoutParams")


@cwp.init_state
def setUp2():
    return {"value": 0}


@cwp.infer("add")
def plus2(state, value):
    return cwp.params["multiplier"] * (state["value"] + value)


@cwp.fit("add", batch_size=1)
def increment2(state, values, infer_results):
    return {"value": state["value"] + sum(values)}


def test_params_not_exist():
    c_instance = cwp()
    with pytest.raises(KeyError):
        assert c_instance.run(add=1, force_fit=True) == 2

    c_instance.shutdown()
