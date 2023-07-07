from motion import Component

import pytest


def test_bad_serve_component():
    with pytest.raises(ValueError):
        c = Component("BadserveComponent")

        @c.init_state
        def setUp():
            return {"value": 0}

        @c.serve("add")
        def plus(state, props):
            return state["value"] + props["value"]

        @c.serve("add")
        def plus2(state, props):
            return state["value"] + props["value"]


c = Component(name="DoubleFit", params={})


@c.init_state
def setUp():
    return {"value": 0}


@c.update("add")
def plus(state, props):
    return {"value": state["value"] + props["value"]}


@c.update("add")
def plus2(state, props):
    return {"value": state["value"] + props["value"]}


@c.serve("read")
def read(state, props):
    return props["value"]


def test_double_fit_component():
    c_instance = c()

    c_instance.run("add", props={"value": 1}, flush_update=True)

    assert c_instance.run("read", props={"value": 2}) == 2
    c_instance.shutdown()
