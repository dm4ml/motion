import hashlib
import logging
import os
import random
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import cloudpickle
import colorlog
import redis
from pydantic import BaseModel

logger = logging.getLogger(__name__)

DEFAULT_KEY_TTL = 60 * 60 * 24  # 1 day


def hash_object(obj: Any) -> str:
    # Convert the object to a string representation
    obj_str = str(obj).encode("utf-8")

    # Calculate the SHA256 hash
    sha256_hash = hashlib.sha256(obj_str)

    # Get the hexadecimal representation of the hash
    hex_digest = sha256_hash.hexdigest()

    return hex_digest


class RedisParams(BaseModel):
    host: str
    port: int
    db: int
    password: Optional[str] = None

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("host", os.getenv("MOTION_REDIS_HOST", "localhost"))
        kwargs.setdefault("port", int(os.getenv("MOTION_REDIS_PORT", "6379")))
        kwargs.setdefault("db", int(os.getenv("MOTION_REDIS_DB", "0")))
        kwargs.setdefault("password", os.getenv("MOTION_REDIS_PASSWORD", None))

        super().__init__(**kwargs)


def clear_instance(instance_name: str) -> bool:
    """Clears the state and cached results associated with a component instance.

    Usage:
    ```python
    from motion import clear_instance

    clear_instance("Counter__default")
    ```

    Args:
        instance (str): Instance name of the component to clear.
            In the form `componentname__instancename`.

    Raises:
        ValueError:
            If the instance name is not in the form
            `componentname__instancename`.

    Returns:
        bool: True if the instance existed, False otherwise.
    """
    if "__" not in instance_name:
        raise ValueError("Instance must be in the form `componentname__instancename`.")

    rp = RedisParams()
    redis_con = redis.Redis(host=rp.host, port=rp.port, password=rp.password, db=rp.db)

    # Check if the instance exists
    if not redis_con.exists(f"MOTION_VERSION:{instance_name}"):
        return False

    # Delete the instance state, version, and cached results
    redis_con.delete(f"MOTION_STATE:{instance_name}")
    redis_con.delete(f"MOTION_VERSION:{instance_name}")

    results_to_delete = redis_con.keys(f"MOTION_RESULT:{instance_name}/*")
    queues_to_delete = redis_con.keys(f"MOTION_QUEUE:{instance_name}/*")
    pipeline = redis_con.pipeline()
    for result in results_to_delete:
        pipeline.delete(result)
    for queue in queues_to_delete:
        pipeline.delete(queue)
    pipeline.execute()

    return True


def inspect_state(instance_name: str) -> Dict[str, Any]:
    """
    Returns the state of a component instance.

    Usage:
    ```python
    from motion import inspect_state

    inspect_state("Counter__default")
    ```

    Args:
        instance (str): Instance name of the component to inspect.
            In the form `componentname__instancename`.

    Raises:
        ValueError:
            If the instance name is not in the form
            `componentname__instancename` or if the instance does not exist.

    Returns:
        Dict[str, Any]: The state of the component instance.
    """
    if "__" not in instance_name:
        raise ValueError("Instance must be in the form `componentname__instancename`.")

    rp = RedisParams()
    redis_con = redis.Redis(host=rp.host, port=rp.port, password=rp.password, db=rp.db)

    # Check if the instance exists
    if not redis_con.exists(f"MOTION_VERSION:{instance_name}"):
        raise ValueError(f"Instance {instance_name} does not exist.")

    # Get the state
    state = loadState(redis_con, instance_name, None)
    return state


class CustomDict(dict):
    def __init__(
        self,
        component_name: str,
        dict_type: str,
        instance_id: Optional[str] = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.component_name = component_name
        self.instance_id = instance_id
        self.dict_type = dict_type
        super().__init__(*args, **kwargs)

    def __getitem__(self, key: str) -> object:
        try:
            return super().__getitem__(key)
        except KeyError:
            raise KeyError(
                f"Key `{key}` not found in {self.dict_type} for "
                + f"instance {self.component_name}__{self.instance_id}."
            )


def validate_args(parameters: Any, op: str) -> bool:
    expected_args = (
        ["state", "values", "infer_results"] if op == "fit" else ["state", "value"]
    )
    if len(parameters) != len(expected_args):
        return False

    for param_name, _ in parameters.items():
        if param_name not in expected_args:
            return False

    return True


def configureLogging(level: str) -> None:
    handler = logging.StreamHandler()

    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s %(levelname)-8s%(reset)s %(blue)s%(message)s",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
    )
    handler.setFormatter(formatter)
    logger = logging.getLogger("motion")
    logger.addHandler(handler)
    logger.setLevel(level)


def loadState(
    redis_con: redis.Redis,
    instance_name: str,
    load_state_func: Optional[Callable],
) -> CustomDict:
    # Get state from redis
    state = CustomDict(
        instance_name.split("__")[0], "state", instance_name.split("__")[1], {}
    )
    loaded_state = redis_con.get(f"MOTION_STATE:{instance_name}")

    if not loaded_state:
        # This is an error
        logger.warning(f"Could not find state for {instance_name}.")
        return state

    # Unpickle state
    loaded_state = cloudpickle.loads(loaded_state)
    if load_state_func is not None:
        state.update(load_state_func(loaded_state))
    else:
        state.update(loaded_state)

    return state


def saveState(
    state_to_save: CustomDict,
    redis_con: redis.Redis,
    instance_name: str,
    save_state_func: Optional[Callable],
) -> None:
    # Save state to redis
    if save_state_func is not None:
        state_to_save = save_state_func(state_to_save)

    state_pickled = cloudpickle.dumps(state_to_save)

    redis_con.set(f"MOTION_STATE:{instance_name}", state_pickled)
    redis_con.incr(f"MOTION_VERSION:{instance_name}")


class FitEvent:
    """Waits for a fit operation to finish."""

    def __init__(self, redis_con: redis.Redis, channel: str, identifier: str) -> None:
        self.channel = channel
        self.pubsub = redis_con.pubsub()
        self.identifier = identifier
        self.pubsub.subscribe(channel)

    def wait(self) -> None:
        for message in self.pubsub.listen():
            if message["type"] != "message":
                continue

            error_data = eval(message["data"])
            identifier = error_data["identifier"]
            exception_str = error_data["exception"]

            if identifier != self.identifier:
                continue

            if exception_str:
                raise RuntimeError(exception_str)

            break


class FitEventGroup:
    """Stores the events for fit operations on a given key."""

    def __init__(self, key: str) -> None:
        self.key = key
        self.events: Dict[str, FitEvent] = {}

    def add(self, udf_name: str, event: FitEvent) -> None:
        self.events[udf_name] = event

    def wait(self) -> None:
        """Waits for all fit operations for this dataflow key
        to finish. Be careful not to trigger an infinite wait if the batch_size
        has not been hit yet!

        Example usage:
        ```python
        from motion import Component

        c = Component("MyComponent")

        @c.init_state
        def setUp():
            return {"state_val": 0, "state_val2": 0}

        @c.fit("my_key")
        def fit1(state, values, infer_results):
            return {"state_val": state["state_val"] + sum(values)}

        @c.fit("my_key")
        def fit2(state, values, infer_results):
            return {"state_val2": state["state_val2"] + sum(values)}

        result, fit_tasks = c.run(my_key=1)
        print(result) # None because no infer op was hit
        fit_tasks.wait() # Wait for all fit tasks to finish
        # Now `state["state_val"] = 1` and `state["state_val2"] = 1`
        ```
        """
        for event in self.events.values():
            event.wait()

    def __str__(self) -> str:
        return f"FitEventGroup(key={self.key}, events={self.events})"

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
