from motion import Component

# Test a pipeline with multiple components
a = Component("ComponentA")


@a.init_state
def setUp():
    return {"value": 0}


@a.infer("add")
def plus(state, value):
    return state["value"] + value


@a.fit("add", batch_size=1)
def increment(state, values, infer_results):
    return {"value": state["value"] + sum(values)}


b = Component("ComponentB")


@b.init_state
def setUp():
    return {"message": ""}


@b.infer("concat")
def concat_message(state, value):
    return state["message"] + " " + value


@b.fit("concat", batch_size=1)
def update_message(state, values, infer_results):
    return {"message": state["message"] + " " + " ".join(values)}


def test_simple_pipeline():
    a_instance = a()
    b_instance = b()
    add_result = a_instance.run(add=1, flush_fit=True)
    assert add_result == 1

    concat_result = b_instance.run(concat=str(add_result), flush_fit=True)
    assert concat_result == " 1"

    add_result_2 = a_instance.run(add=2)
    assert add_result_2 == 3
    concat_result_2 = b_instance.run(concat=str(add_result_2))
    assert concat_result_2 == " 1 3"

    # Must do this or you get hanging processes!
    a_instance.shutdown()
    b_instance.shutdown()
