from motion import Component

Counter = Component("Counter")


@Counter.init_state
def setup():
    return {"value": 1}


@Counter.infer("multiply")
def noop(state, value):
    return state["value"] * value


@Counter.fit("multiply", batch_size=1)
def increment(state, values, infer_results):
    print(state["does_not_exist"])  # This should break thread
    return {"value": state["value"] + 1}


def test_release_lock_on_broken_fit():
    c = Counter("same_id")
    c.run(multiply=2, flush_fit=True)
    c.shutdown()

    # Should be able to run again
    c2 = Counter("same_id")
    c2.run(multiply=2)
    c2.shutdown()
