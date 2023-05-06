from motion import Component

# Test a pipeline with multiple components


class ComponentA(Component):
    def setUp(self):
        return {"value": 0}

    @Component.infer("add")
    def plus(self, state, value):
        return state["value"] + value

    @Component.fit("add", batch_size=1)
    def increment(self, state, values, infer_results):
        return {"value": state["value"] + sum(values)}


class ComponentB(Component):
    def setUp(self):
        return {"message": ""}

    @Component.infer("concat")
    def concat_message(self, state, value):
        return state["message"] + " " + value

    @Component.fit("concat", batch_size=1)
    def update_message(self, state, values, infer_results):
        return {"message": state["message"] + " " + " ".join(values)}


def test_simple_pipeline():
    a = ComponentA()
    b = ComponentB()

    add_result = a.run(add=1, wait_for_fit=True)
    assert add_result == 1

    concat_result = b.run(concat=str(add_result), wait_for_fit=True)
    assert concat_result == " 1"

    add_result_2 = a.run(add=2)
    assert add_result_2 == 3
    concat_result_2 = b.run(concat=str(add_result_2))
    assert concat_result_2 == " 1 3"
