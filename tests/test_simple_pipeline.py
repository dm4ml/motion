from motion import Component
from motion.utils import get_component_instance_usage

# Test a pipeline with multiple components
a = Component("ComponentA")


@a.init_state
def setUp():
    return {"value": 0}


@a.serve("add")
def plus(state, props):
    props["something_else"] = "Hello"
    return state["value"] + props["value"]


@a.update("add")
def increment(state, props):
    assert "something_else" in props
    return {"value": state["value"] + props["value"]}


b = Component("ComponentB")


@b.init_state
def setUp():
    return {"message": ""}


@b.serve("concat")
def concat_message(state, props):
    return state["message"] + " " + props["str_to_concat"]


@b.update("concat")
def update_message(state, props):
    return {"message": state["message"] + " " + props["str_to_concat"]}


def test_simple_pipeline():
    a_instance = a("my_instance_a")
    b_instance = b("my_instance_b")
    add_result = a_instance.run("add", props={"value": 1}, flush_update=True)
    assert add_result == 1

    concat_result = b_instance.run(
        "concat", props={"str_to_concat": str(add_result)}, flush_update=True
    )
    assert concat_result == " 1"

    add_result_2 = a_instance.run("add", props={"value": 2})
    assert add_result_2 == 3
    concat_result_2 = b_instance.run(
        "concat", props={"str_to_concat": str(add_result_2)}
    )
    assert concat_result_2 == " 1 3"

    # Check that the logs show results
    a_instance.shutdown()
    b_instance.shutdown()

    usage = get_component_instance_usage("ComponentA", "my_instance_a")
    assert usage.keys() == {"version", "resultsByFlow", "allResults"}
