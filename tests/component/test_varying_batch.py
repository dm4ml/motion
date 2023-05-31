from motion import Component

c = Component("VaryingBatch")


@c.init_state
def setUp():
    return {"value": 0}


@c.fit("route1", batch_size=1)
def increment(state, values, infer_results):
    return {"value": state["value"] + len(values)}


@c.fit("route2", batch_size=10)
def increment2(state, values, infer_results):
    return {"value": state["value"] + len(values)}


@c.fit("route3", batch_size=100)
def increment3(state, values, infer_results):
    return {"value": state["value"] + len(values)}


def test_varying_batch():
    # Test batch_size=1
    c_instance = c()
    c_instance.run(route1=1, force_fit=True)
    assert c_instance.read_state("value") == 1

    for _ in range(9):
        c_instance.run(route2=1)

    c_instance.run(route2=1, force_fit=True)
    assert c_instance.read_state("value") == 11

    for _ in range(99):
        c_instance.run(route3=1)

    c_instance.run(route3=1, force_fit=True)
    assert c_instance.read_state("value") == 111
