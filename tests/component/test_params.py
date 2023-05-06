from motion import Component


class ComponentWithParams(Component):
    def setUp(self):
        return {"value": 0}

    @Component.infer("add")
    def plus(self, state, value):
        return self.params["multiplier"] * (state["value"] + value)

    @Component.fit("add", batch_size=1)
    def increment(self, state, values, infer_results):
        return {"value": state["value"] + sum(values)}


def test_params():
    c = ComponentWithParams(params={"multiplier": 2})

    assert c.run(add=1, wait_for_fit=True) == 2
    assert c.run(add=2, wait_for_fit=True) == 6
