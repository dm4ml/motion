import subprocess
import pytest
import os
import redis

from motion.utils import RedisParams


def test_dev_instance_cleanup():
    code_to_execute = """
from motion import Component

testc = Component("testc")

@testc.init_state
def setup():
    return {"value": 1}

@testc.serve("multiply")
def noop(state, props):
    return state["value"] * props["value"]
    
if __name__ == "__main__":
    i = testc("hello")
    res = i.run("multiply", props={"value": 2}, ignore_cache=True)
"""

    # Copy env vars from the current process
    env = os.environ.copy()

    # Set the MOTION_ENV variable to dev
    env["MOTION_ENV"] = "dev"

    # Run the code in a separate Python process
    subprocess.run(["python", "-c", code_to_execute], env=env)

    # Check that "hello" instance does not exist
    rp = RedisParams()
    r = redis.Redis(
        host=rp.host,
        port=rp.port,
        password=rp.password,
        db=rp.db,
    )
    assert r.get("MOTION_VERSION:DEV:testc__hello") is None, "Instance was not deleted."
