import redis
import psutil
import pytest
import os
import logging


@pytest.fixture(scope="session", autouse=True)
def redis_fixture():
    """Set up redis as a pytest fixture."""

    # Change dir to the root of tests
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    from motion import RedisParams
    from motion.utils import import_config

    import_config()

    rp = RedisParams()
    r = redis.Redis(
        host=rp.host,
        port=rp.port,
        password=rp.password,
        db=rp.db,
    )
    assert os.getenv("MOTION_ENV") == "prod", "MOTION_ENV must be set to prod."

    try:
        r.ping()
    except redis.exceptions.ConnectionError:
        raise ConnectionError("Make sure you are running Redis on localhost:6381.")

    r.flushdb()
    assert len(r.keys()) == 0

    yield r

    # Delete any parquet files that were created
    home = os.path.expanduser("~")
    parquet_dir = f"{home}/.motion"

    # Delete any files in the parquet directory
    for filename in os.listdir(parquet_dir):
        file_path = os.path.join(parquet_dir, filename)
        # Delete the file
        os.remove(file_path)


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config, items):
    # Print process count before test execution starts
    print_process_count()


@pytest.hookimpl(trylast=True)
def pytest_unconfigure(config):
    # Print process count after test execution ends
    print_process_count()


def print_process_count():
    count = sum(1 for _ in psutil.process_iter())
    print(f"Number of processes: {count}")
