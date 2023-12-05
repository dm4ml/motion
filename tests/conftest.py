import redis
import psutil
import pytest
import os
import yaml

from motion.utils import RedisParams


@pytest.fixture(scope="session", autouse=True)
def redis_fixture():
    """Set up redis as a pytest fixture."""
    # Set env vars
    os.environ["MOTION_REDIS_HOST"] = "localhost"
    os.environ["MOTION_REDIS_PORT"] = "6381"
    os.environ.pop("MOTION_REDIS_PASSWORD", None)
    os.environ["MOTION_REDIS_DB"] = "0"

    config = None
    config_file = "config.yaml"
    if os.path.isfile(config_file):
        with open(config_file, "r") as file:
            config = yaml.safe_load(file)

    rp = RedisParams(config=config)
    r = redis.Redis(
        host=rp.host,
        port=rp.port,
        password=rp.password,
        db=rp.db,
    )
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
