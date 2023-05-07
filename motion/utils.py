import logging
import threading
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


def validate_args(parameters: Dict, op: str) -> bool:
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
    def __init__(self, key: str) -> None:
        self.key = key
        self.events: Dict[str, threading.Event] = {}

    def add(self, udf_name: str, event: threading.Event) -> None:
        self.events[udf_name] = event

    def wait(self) -> None:
        for event in self.events.values():
            event.wait()

    def __str__(self) -> str:
        return f"FitEventGroup(key={self.key}, events={self.events})"

    def __repr__(self) -> str:
        return self.__str__()
