from motion import Component

import pytest

c = Component("ComponentWithParams", params={"multiplier": 2})


@c.init_state
def setUp():
    return {"value": 0}


@c.serve("add")
def plus(state, props):
    return c.params["multiplier"] * (state["value"] + props["value"])


@c.update("add")
def increment(state, props):
    return {"value": state["value"] + props["value"]}


def test_params():
    c_instance = c()
    assert c_instance.run("add", props={"value": 1}, flush_update=True) == 2
    assert c_instance.run("add", props={"value": 2}, flush_update=True) == 6


cwp = Component("ComponentWithoutParams")


@cwp.init_state
def setUp2():
    return {"value": 0}


@cwp.serve("add")
def plus2(state, props):
    return cwp.params["multiplier"] * (state["value"] + props["value"])


@cwp.update("add")
def increment2(state, props):
    return {"value": state["value"] + props["value"]}


def test_params_not_exist():
    c_instance = cwp()
    with pytest.raises(KeyError):
        assert (
            c_instance.run("add", props={"value": 1}, flush_update=True) == 2
        )

    c_instance.shutdown()
