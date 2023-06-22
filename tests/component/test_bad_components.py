from motion import Component

import pytest


def test_bad_infer_component():
    with pytest.raises(ValueError):
        c = Component("BadInferComponent")

        @c.init_state
        def setUp():
            return {"value": 0}

        @c.infer("add")
        def plus(state, value):
            return state["value"] + value

        @c.infer("add")
        def plus2(state, value):
            return state["value"] + value


c = Component(name="DoubleFit", params={})


@c.init_state
def setUp():
    return {"value": 0}


@c.fit("add")
def plus(state, value, infer_result):
    return {"value": state["value"] + value}


@c.fit("add")
def plus2(state, value, infer_result):
    return {"value": state["value"] + value}


@c.infer("read")
def read(state, value):
    return state["value"]


def test_double_fit_component():
    c_instance = c()

    c_instance.run("add", kwargs={"value": 1}, flush_fit=True)

    assert c_instance.run("read", kwargs={"value": 2}) == 2
    c_instance.shutdown()
