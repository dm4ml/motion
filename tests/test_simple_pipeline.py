from motion import Component

# Test a pipeline with multiple components
a = Component("ComponentA")


@a.init_state
def setUp():
    return {"value": 0}


@a.infer("add")
def plus(state, value):
    return state["value"] + value


@a.fit("add")
def increment(state, value, infer_result):
    return {"value": state["value"] + value}


b = Component("ComponentB")


@b.init_state
def setUp():
    return {"message": ""}


@b.infer("concat")
def concat_message(state, str_to_concat):
    return state["message"] + " " + str_to_concat


@b.fit("concat")
def update_message(state, str_to_concat, infer_result):
    return {"message": state["message"] + " " + str_to_concat}


def test_simple_pipeline():
    a_instance = a()
    b_instance = b()
    add_result = a_instance.run("add", kwargs={"value": 1}, flush_fit=True)
    assert add_result == 1

    concat_result = b_instance.run(
        "concat", kwargs={"str_to_concat": str(add_result)}, flush_fit=True
    )
    assert concat_result == " 1"

    add_result_2 = a_instance.run("add", kwargs={"value": 2})
    assert add_result_2 == 3
    concat_result_2 = b_instance.run(
        "concat", kwargs={"str_to_concat": str(add_result_2)}
    )
    assert concat_result_2 == " 1 3"

    # Must do this or you get hanging processes!
    a_instance.shutdown()
    b_instance.shutdown()
