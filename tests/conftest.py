import redis
import pytest
import os

from motion.utils import RedisParams


@pytest.fixture(scope="module", autouse=True)
def redis_fixture():
    """Set up redis as a pytest fixture."""
    # Set env vars
    os.environ["MOTION_REDIS_HOST"] = "localhost"
    os.environ["MOTION_REDIS_PORT"] = "6381"
    os.environ.pop("MOTION_REDIS_PASSWORD", None)
    os.environ["MOTION_REDIS_DB"] = "0"

    rp = RedisParams()
    r = redis.Redis(
        host=rp.host,
        port=rp.port,
        password=rp.password,
        db=rp.db,
    )
    try:
        r.ping()
    except redis.exceptions.ConnectionError:
        raise ConnectionError(
            "Make sure you are running Redis on localhost:6381."
        )

    r.flushdb()

    yield
