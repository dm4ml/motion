from motion import Component
import pytest
import multiprocessing

from motion.utils import RedisParams
import redis

Counter = Component("Counter", cache_ttl=0)


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
    assert c.run("number", props={"value": 1}) == (0, 1)


def test_disabled():
    # Create disabled component
    c = Counter(disabled=True)
    with pytest.raises(RuntimeError):
        c.run("number", props={"value": 1})

    # Make sure this can run successfully
    process = multiprocessing.Process(target=counter_process)
    process.start()
    process.join()


def test_no_caching():
    # Create component with no caching
    c = Counter("no_cache_test")
    assert c._cache_ttl == 0
    assert c._executor._cache_ttl == 0
    c.run("number", props={"value": 1}, flush_update=True)

    # Check that the result is not in the cache
    rp = RedisParams()
    r = redis.Redis(
        host=rp.host,
        port=rp.port,
        password=rp.password,
        db=rp.db,
    )
    # Define the prefix
    prefix = "MOTION_RESULT:Counter__no_cache_test/"

    # Initialize the count
    count = 0

    # Iterate over keys with the prefix
    for _ in r.scan_iter(f"{prefix}*"):
        count += 1

    # Check that there are no keys with the prefix
    assert count == 0
