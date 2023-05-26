from motion import Component


def test_redis_saving(redisdb):
    Counter = Component("Counter")

    @Counter.init_state
    def setup():
        return {"value": 1}

    @Counter.infer("multiply")
    def noop(state, value):
        return state["value"] * value

    @Counter.fit("multiply", batch_size=1)
    def increment(state, values, infer_results):
        return {"value": state["value"] + 1}

    inst1 = Counter(name="test", redis_con=redisdb)
    assert inst1.run(multiply=2, wait_for_fit=True) == 2
    assert inst1.read_state("value") == 2
    inst1.shutdown()

    inst2 = Counter(name="test", redis_con=redisdb)
    assert inst2.read_state("value") == 2
    assert inst2.run(multiply=2, wait_for_fit=True) == 4
    assert inst2.read_state("value") == 3
    inst2.shutdown()
