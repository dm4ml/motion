from motion import Component

Counter = Component("Counter")


@Counter.init_state
def setup():
    return {"value": 1}


@Counter.serve("multiply")
def noop(state, props):
    return state["value"] * props["value"]


@Counter.update("multiply")
def increment(state, props):
    return {"value": state["value"] + 1}


def test_redis_saving():
    inst1 = Counter(instance_id="test")
    assert inst1.run("multiply", props={"value": 2}, flush_update=True) == 2
    assert inst1.read_state("value") == 2
    inst1.shutdown()

    print("Starting second instance")

    inst2 = Counter(instance_id="test")
    assert inst2.read_state("value") == 2
    assert inst2.run("multiply", props={"value": 3}, flush_update=True) == 6
    assert inst2.read_state("value") == 3
    inst2.shutdown()
