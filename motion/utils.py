import hashlib
import logging
import os
import random
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import cloudpickle
import colorlog
import redis
import yaml
from pydantic import BaseModel

from motion.dicts import State

logger = logging.getLogger(__name__)

DEFAULT_KEY_TTL = 60 * 60 * 24  # 1 day


def import_config(config_path: str = ".motionrc.yml") -> None:
    # If env var MOTION_YAML_LOADED is not set, load .motionrc.yml
    if os.getenv("MOTION_YAML_LOADED") is None:
        # Load .motionrc.yml if it exists
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)

            # Set env vars to the values in .motionrc.yml
            for k, v in config.items():
                os.environ[k] = str(v)

        os.environ["MOTION_YAML_LOADED"] = "1"


def hash_object(obj: Any) -> str:
    # Convert the object to a string representation
    obj_str = str(obj).encode("utf-8")

    # Calculate the SHA256 hash
    sha256_hash = hashlib.sha256(obj_str)

    # Get the hexadecimal representation of the hash
    hex_digest = sha256_hash.hexdigest()

    return hex_digest


class RedisParams(BaseModel, extra="allow"):
    host: str
    port: int
    db: int
    password: Optional[str] = None
    ssl: bool = False

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("host", os.getenv("MOTION_REDIS_HOST", "localhost"))
        kwargs.setdefault("port", int(os.getenv("MOTION_REDIS_PORT", "6379")))
        kwargs.setdefault("db", int(os.getenv("MOTION_REDIS_DB", "0")))

        if str(os.getenv("MOTION_REDIS_PASSWORD", "None")) != "None":
            kwargs["password"] = os.getenv("MOTION_REDIS_PASSWORD")

        if str(os.getenv("MOTION_REDIS_SSL", "False")) == "True":
            kwargs["ssl"] = True

        super().__init__(**kwargs)


def get_redis_params() -> RedisParams:
    rp = RedisParams()
    return rp


def get_instances(component_name: str) -> List[str]:
    """Gets all instances of a component.

    Args:
        component_name (str): Name of the component.

    Returns:
        List[str]: List of instance ids.
    """
    rp = get_redis_params()
    redis_con = redis.Redis(**rp.dict())

    # Scan for all keys with prefix
    prefix = f"MOTION_VERSION:{component_name}__*"
    instance_ids = []
    for key in redis_con.scan_iter(prefix):
        instance_ids.append(key.decode("utf-8").split("__")[1])  # type: ignore

    redis_con.close()

    return instance_ids


def clear_dev_instances() -> int:
    """Clears all dev instances."""
    rp = get_redis_params()
    redis_con = redis.Redis(**rp.dict())

    # Scan for all keys with prefix
    prefix = "MOTION_VERSION:DEV:*"
    pipeline = redis_con.pipeline()
    num_keys_deleted = 0
    for key in redis_con.scan_iter(prefix):
        pipeline.delete(key)
        num_keys_deleted += 1

    # Delete all states too
    prefix = "MOTION_STATE:DEV:*"
    for key in redis_con.scan_iter(prefix):
        pipeline.delete(key)

    results_to_delete = redis_con.keys("MOTION_RESULT:DEV:*")
    queues_to_delete = redis_con.keys("MOTION_QUEUE:DEV:*")
    locks_to_delete = redis_con.keys("MOTION_LOCK:DEV:*")
    for result in results_to_delete:
        pipeline.delete(result)
    for queue in queues_to_delete:
        pipeline.delete(queue)
    for lock in locks_to_delete:
        pipeline.delete(lock)

    pipeline.execute()
    pipeline.close()

    return num_keys_deleted


def clear_instance(instance_name: str) -> bool:
    """Clears the state and cached results associated with a component instance.

    Usage:
    ```python
    from motion import clear_instance

    clear_instance("Counter__default")
    ```

    Args:
        instance_name (str): Instance name of the component to clear.
            In the form `componentname__instanceid`.

    Raises:
        ValueError:
            If the instance name is not in the form
            `componentname__instanceid`.

    Returns:
        bool: True if the instance existed, False otherwise.
    """
    if "__" not in instance_name:
        raise ValueError("Instance must be in the form `componentname__instanceid`.")

    rp = get_redis_params()
    redis_con = redis.Redis(
        **rp.dict(),
    )

    # Check if the instance exists
    if not redis_con.exists(f"MOTION_VERSION:{instance_name}") and not redis_con.exists(
        f"MOTION_VERSION:DEV:{instance_name}"
    ):
        return False

    # Delete the instance state, version, and cached results
    redis_con.delete(f"MOTION_STATE:{instance_name}")
    redis_con.delete(f"MOTION_STATE:DEV:{instance_name}")
    redis_con.delete(f"MOTION_VERSION:DEV:{instance_name}")
    redis_con.delete(f"MOTION_VERSION:{instance_name}")
    redis_con.delete(f"MOTION_LOCK:{instance_name}")
    redis_con.delete(f"MOTION_LOCK:DEV:{instance_name}")

    for env in [":DEV", ""]:
        results_to_delete = redis_con.keys(f"MOTION_RESULT{env}:{instance_name}/*")
        queues_to_delete = redis_con.keys(f"MOTION_QUEUE{env}:{instance_name}/*")
        channels_to_delete = redis_con.keys(f"MOTION_CHANNEL{env}:{instance_name}/*")

        pipeline = redis_con.pipeline()
        for result in results_to_delete:
            pipeline.delete(result)
        for queue in queues_to_delete:
            pipeline.delete(queue)
        for channel in channels_to_delete:
            pipeline.delete(channel)

    pipeline.execute()

    redis_con.close()

    return True


def inspect_state(instance_name: str) -> Optional[State]:
    """
    Returns the state of a component instance.

    Usage:
    ```python
    from motion import inspect_state

    inspect_state("Counter__default")
    ```

    Args:
        instance_name (str): Instance name of the component to inspect.
            In the form `componentname__instanceid`.

    Raises:
        ValueError:
            If the instance name is not in the form
            `componentname__instanceid` or if the instance does not exist.

    Returns:
        State: The state of the component instance.
    """
    if "__" not in instance_name:
        raise ValueError("Instance must be in the form `componentname__instanceid`.")

    rp = get_redis_params()
    redis_con = redis.Redis(
        **rp.dict(),
    )

    # Check if the instance exists
    if not redis_con.exists(f"MOTION_VERSION:{instance_name}"):
        raise ValueError(f"Instance {instance_name} does not exist.")

    # Get the state
    state, _ = loadState(redis_con, instance_name, None)

    redis_con.close()
    return state


def validate_args(parameters: Any, op: str) -> bool:
    if "state" not in parameters.keys():
        return False

    if "props" not in parameters.keys():
        return False

    if len(parameters.keys()) != 2:
        return False

    return True


def configureLogging(level: str) -> None:
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s %(levelname)-8s%(reset)s %(blue)s%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
    )

    logger = logging.getLogger("motion")
    if logger.hasHandlers():
        logger.handlers.clear()

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.setLevel(level)


def loadState(
    redis_con: redis.Redis,
    instance_name: str,
    load_state_func: Optional[Callable],
) -> Tuple[Optional[State], int]:
    # Get state from redis
    state = State(instance_name.split("__")[0], instance_name.split("__")[1], {})

    # If dev mode, load with diff prefix
    loaded_state = None
    version = None
    if os.getenv("MOTION_ENV", "dev") == "dev":
        loaded_state = redis_con.get(f"MOTION_STATE:DEV:{instance_name}")
        if loaded_state:
            v_identifier = f"MOTION_VERSION:DEV:{instance_name}"
            version = int(redis_con.get(v_identifier))  # type: ignore
        else:
            loaded_state = redis_con.get(f"MOTION_STATE:{instance_name}")

    else:
        loaded_state = redis_con.get(f"MOTION_STATE:{instance_name}")

    if not loaded_state:
        # This is an error
        logger.warning(f"Could not find state for {instance_name}. Creating new state.")
        return None, 0

    if not version:
        version = int(redis_con.get(f"MOTION_VERSION:{instance_name}"))  # type: ignore

    # Unpickle state
    loaded_state = cloudpickle.loads(loaded_state)

    if load_state_func is not None:
        state.update(load_state_func(loaded_state))
    else:
        state.update(loaded_state)

    return state, version


def saveState(
    state_to_save: State,
    version: int,
    redis_con: redis.Redis,
    instance_name: str,
    save_state_func: Optional[Callable],
) -> int:
    # If the version in redis is greater than this version, drop the save
    redis_v = None
    if os.getenv("MOTION_ENV", "dev") == "dev":
        redis_con.get(f"MOTION_VERSION:DEV:{instance_name}")

    if not redis_v:
        redis_v = redis_con.get(f"MOTION_VERSION:{instance_name}")

    if redis_v and int(redis_v) > version:
        # This means that another process has already saved the state
        # Return a sentinel value that indicates that the state was not saved
        return -1

    # Save state to redis
    if save_state_func is not None:
        state_to_save = save_state_func(state_to_save)

    state_pickled = cloudpickle.dumps(state_to_save)

    if os.getenv("MOTION_ENV", "dev") == "dev":
        redis_con.set(f"MOTION_STATE:DEV:{instance_name}", state_pickled)
        redis_con.set(f"MOTION_VERSION:DEV:{instance_name}", version + 1)

    else:
        redis_con.set(f"MOTION_STATE:{instance_name}", state_pickled)
        redis_con.set(f"MOTION_VERSION:{instance_name}", version + 1)

    return version + 1


class UpdateEvent:
    """Waits for a update operation to finish."""

    def __init__(self, redis_con: redis.Redis, channel: str, identifier: str) -> None:
        self.channel = channel
        self.pubsub = redis_con.pubsub()
        self.identifier = identifier
        self.pubsub.subscribe(channel)

    def wait(self) -> None:
        for message in self.pubsub.listen():
            if message["type"] != "message":
                continue

            message_data_str = message["data"].decode("utf-8")
            if message_data_str[0] == "{":
                error_data = eval(message["data"])
                identifier = error_data["identifier"]
                exception_str = error_data["exception"]

                if identifier != self.identifier:
                    continue

                if exception_str:
                    raise RuntimeError(exception_str)

                break

            else:
                if message_data_str == self.identifier:
                    break


class UpdateEventGroup:
    """Stores the events for update operations on a given key."""

    def __init__(self, key: str) -> None:
        self.key = key
        self.events: Dict[str, UpdateEvent] = {}

    def add(self, udf_name: str, event: UpdateEvent) -> None:
        self.events[udf_name] = event

    def wait(self) -> None:
        """Waits for all update operations for this flow key
        to finish.

        Example usage:
        ```python
        from motion import Component

        c = Component("MyComponent")

        @c.init_state
        def setUp():
            return {"state_val": 0, "state_val2": 0}

        @c.update("my_key")
        def fit1(state, props, value):
            return {"state_val": state["state_val"] + value}

        @c.update("my_key")
        def fit2(state, props, value):
            return {"state_val2": state["state_val2"] + value}

        result, fit_tasks = c.run(my_key=1)
        print(result) # None because no serve op was hit
        fit_tasks.wait() # Wait for all update tasks to finish
        # Now `state["state_val"] = 1` and `state["state_val2"] = 1`
        ```
        """
        for event in self.events.values():
            event.wait()

    def __str__(self) -> str:
        return f"UpdateEventGroup(key={self.key}, events={self.events})"

    def __repr__(self) -> str:
        return self.__str__()


def random_passphrase(num_words: int = 3) -> str:
    """Generate random passphrase from eff wordlist."""

    # Open wordlist
    f = Path(__file__).resolve().parent / "res" / "eff_short_wordlist_1.txt"
    wordlist = {
        roll: word
        for roll, word in (
            line.split("\t") for line in f.read_text().split(os.linesep) if "\t" in line
        )
    }

    dice_rolls = [
        "".join(f"{random.SystemRandom().randint(1, 6)}" for _ in range(4))
        for _ in range(num_words)
    ]
    words = [wordlist[roll] for roll in dice_rolls]
    return "-".join(words)
