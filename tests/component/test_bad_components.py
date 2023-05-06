from motion import Component

import pytest


class BadInferComponent(Component):
    def setUp(self):
        return {"value": 0}

    @Component.infer("add")
    def plus(self, state, value):
        return state["value"] + value

    @Component.infer("add")
    def plus2(self, state, value):
        return state["value"] + value


class BadFitComponent(Component):
    def setUp(self):
        return {"value": 0}

    @Component.fit("add")
    def plus(self, state, value):
        return state["value"] + value

    @Component.fit("add")
    def plus2(self, state, value):
        return state["value"] + value


def test_bad_component():
    with pytest.raises(ValueError):
        c = BadInferComponent()

    with pytest.raises(ValueError):
        c = BadFitComponent()
