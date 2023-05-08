from motion import Component

import pytest


def test_bad_infer_component():
    with pytest.raises(ValueError):
        c = Component("BadInferComponent")

        @c.init
        def setUp():
            return {"value": 0}

        @c.infer("add")
        def plus(state, value):
            return state["value"] + value

        @c.infer("add")
        def plus2(state, value):
            return state["value"] + value


def test_double_fit_component():
    c = Component(name="DoubleFit", params={})

    @c.init
    def setUp():
        return {"value": 0}

    @c.fit("add")
    def plus(state, values, infer_results):
        return {"value": state["value"] + sum(values)}

    @c.fit("add")
    def plus2(state, values, infer_results):
        return {"value": state["value"] + sum(values)}

    @c.infer("read")
    def read(state, value):
        return state["value"]

    c.run(add=1, wait_for_fit=True)
    assert c.run(read=1) == 2
