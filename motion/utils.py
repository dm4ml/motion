import logging
import os
import random
import threading
from pathlib import Path
from typing import Any, Dict

import colorlog

logger = logging.getLogger(__name__)


class CustomDict(dict):
    def __init__(
        self,
        component_name: str,
        dict_type: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.component_name = component_name
        self.dict_type = dict_type
        super().__init__(*args, **kwargs)

    def __getitem__(self, key: str) -> object:
        try:
            return super().__getitem__(key)
        except KeyError:
            raise KeyError(
                f"Key `{key}` not found in {self.dict_type} for "
                + f"component {self.component_name}."
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


class FitEventGroup:
    """Stores the events for fit operations on a given key."""

    def __init__(self, key: str) -> None:
        self.key = key
        self.events: Dict[str, threading.Event] = {}

    def add(self, udf_name: str, event: threading.Event) -> None:
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
