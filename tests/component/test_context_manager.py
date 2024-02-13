from motion import Component
import pytest

CounterCM = Component("CounterCM")


@CounterCM.init_state
def setUp():
    return {"value": 0, "list_val": [1, 2, 3], "dict_val": {"a": 1, "b": 2}}


@CounterCM.serve("number")
def noop(state, props):
    return state["value"], props["value"]


@CounterCM.update("number")
def increment(state, props):
    return {"value": state["value"] + props["value"]}


def test_context_manager():
    with CounterCM() as c:
        assert c.read_state("value") == 0

        assert c.run("number", props={"value": 1}, flush_update=True)[1] == 1
        c.run("number", props={"value": 2}, flush_update=True)
        assert c.run("number", props={"value": 3}, flush_update=True)[1] == 3
        assert c.run("number", props={"value": 4}, flush_update=True)[0] == 6
