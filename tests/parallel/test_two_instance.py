from motion import Component

Counter = Component("Counter")


@Counter.init_state
def setup():
    return {"value": 1}


@Counter.serve("multiply")
def noop(state, value):
    return state["value"] * value


@Counter.update("multiply")
def increment(state, value, serve_result):
    return {"value": state["value"] + 1}


def test_redis_saving():
    inst1 = Counter(name="test")
    assert inst1.run("multiply", kwargs={"value": 2}, flush_update=True) == 2
    assert inst1.read_state("value") == 2
    inst1.shutdown()

    print("Starting second instance")

    inst2 = Counter(name="test")
    assert inst2.read_state("value") == 2
    assert inst2.run("multiply", kwargs={"value": 3}, flush_update=True) == 6
    assert inst2.read_state("value") == 3
    inst2.shutdown()
