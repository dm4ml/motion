from motion import Component
import pytest
import multiprocessing

Counter = Component("Counter")


@Counter.init_state
def setUp():
    return {"value": 0}


@Counter.infer("number")
def noop(state, value):
    return state["value"], value


@Counter.fit("number", batch_size=1)
def increment(state, values, infer_results):
    return {"value": state["value"] + sum(values)}


# Create enabled component in a subprocess
def counter_process():
    c = Counter()
    assert c.run(number=1) == (0, 1)


def test_disabled():
    # Create disabled component
    c = Counter(disabled=True)
    with pytest.raises(RuntimeError):
        c.run(number=1)

    process = multiprocessing.Process(target=counter_process)
    process.start()
    process.join()
