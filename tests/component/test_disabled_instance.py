from motion import Component
import pytest
import multiprocessing

Counter = Component("Counter")


@Counter.init_state
def setUp():
    return {"value": 0}


@Counter.serve("number")
def noop(state, props):
    return state["value"], props["value"]


@Counter.update("number")
def increment(state, props):
    return {"value": state["value"] + props["value"]}


# Create enabled component in a subprocess
def counter_process():
    c = Counter()
    assert c.run(number=1) == (0, 1)


def test_disabled():
    # Create disabled component
    c = Counter(disabled=True)
    with pytest.raises(RuntimeError):
        c.run("number", props={"value": 1})

    process = multiprocessing.Process(target=counter_process)
    process.start()
    process.join()
