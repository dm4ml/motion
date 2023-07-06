from motion import Component

import pytest

c = Component("ComponentWithParams", params={"multiplier": 2})


@c.init_state
def setUp():
    return {"value": 0}


@c.serve("add")
def plus(state, value):
    return c.params["multiplier"] * (state["value"] + value)


@c.update("add")
def increment(state, value, serve_result):
    return {"value": state["value"] + value}


def test_params():
    c_instance = c()
    assert c_instance.run("add", kwargs={"value": 1}, flush_update=True) == 2
    assert c_instance.run("add", kwargs={"value": 2}, flush_update=True) == 6


cwp = Component("ComponentWithoutParams")


@cwp.init_state
def setUp2():
    return {"value": 0}


@cwp.serve("add")
def plus2(state, value):
    return cwp.params["multiplier"] * (state["value"] + value)


@cwp.update("add")
def increment2(state, value, serve_result):
    return {"value": state["value"] + value}


def test_params_not_exist():
    c_instance = cwp()
    with pytest.raises(KeyError):
        assert (
            c_instance.run("add", kwargs={"value": 1}, flush_update=True) == 2
        )

    c_instance.shutdown()
