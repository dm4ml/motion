from motion import Component

import pytest


def test_bad_serve_component():
    with pytest.raises(ValueError):
        c = Component("BadserveComponent")

        @c.init_state
        def setUp():
            return {"value": 0}

        @c.serve("add")
        def plus(state, value):
            return state["value"] + value

        @c.serve("add")
        def plus2(state, value):
            return state["value"] + value


c = Component(name="DoubleFit", params={})


@c.init_state
def setUp():
    return {"value": 0}


@c.update("add")
def plus(state, value, serve_result):
    return {"value": state["value"] + value}


@c.update("add")
def plus2(state, value, serve_result):
    return {"value": state["value"] + value}


@c.serve("read")
def read(state, value):
    return state["value"]


def test_double_fit_component():
    c_instance = c()

    c_instance.run("add", kwargs={"value": 1}, flush_update=True)

    assert c_instance.run("read", kwargs={"value": 2}) == 2
    c_instance.shutdown()
