from motion import Component

import pytest

Counter = Component("Counter")


@Counter.init_state
def setup():
    return {"value": 1}


@Counter.serve("multiply")
def noop(state, props):
    return state["value"] * props["value"]


@Counter.update("multiply")
def increment(state, props):
    print(state["does_not_exist"])  # This should break thread
    return {"value": state["value"] + 1}


def test_release_lock_on_broken_update():
    c = Counter("same_id")
    with pytest.raises(RuntimeError):
        c.run("multiply", props={"value": 2}, flush_update=True)
    c.shutdown()

    # Should be able to run again
    c2 = Counter("same_id")
    c2.run("multiply", props={"value": 2})
    c2.shutdown()
