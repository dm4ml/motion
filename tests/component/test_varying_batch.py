from motion import Component


def test_varying_batch():
    c = Component("VaryingBatch")

    @c.init_state
    def setUp():
        return {"value": 0}

    @c.infer("get")
    def get_value(state, value):
        return state["value"]

    @c.fit("route1", batch_size=1)
    def increment(state, values, infer_results):
        return {"value": state["value"] + len(values)}

    @c.fit("route2", batch_size=10)
    def increment2(state, values, infer_results):
        return {"value": state["value"] + len(values)}

    @c.fit("route3", batch_size=100)
    def increment3(state, values, infer_results):
        return {"value": state["value"] + len(values)}

    # Test batch_size=1
    c_instance = c()
    c_instance.run(route1=1, wait_for_fit=True)
    assert c_instance.run(get=True) == 1

    for _ in range(10):
        _, event = c_instance.run(route2=1, return_fit_event=True)

    event.wait()
    assert c_instance.run(get=True) == 11

    for _ in range(100):
        _, event = c_instance.run(route3=1, return_fit_event=True)

    event.wait()
    assert c_instance.run(get=True) == 111
