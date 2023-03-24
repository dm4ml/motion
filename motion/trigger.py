import inspect
import logging
import threading
import sys

from abc import ABC, abstractmethod
from collections import namedtuple
from queue import SimpleQueue

TriggerElement = namedtuple(
    "TriggerElement", ["relation", "identifier", "key", "value"]
)
TriggerFn = namedtuple("TriggerFn", ["name", "fn", "isTransform"])

from motion.utils import logger


class Params(dict):
    def __init__(self, trigger_name, *args, **kwargs):
        self.trigger_name = trigger_name
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError:
            raise KeyError(
                f"Key `{key}` not found in {self.trigger_name} params."
            )


class Trigger(ABC):
    def __init__(self, cursor, name, version, params={}):
        self.name = name

        # Validate number of arguments in each trigger and set up routes
        route_list = self.routes()
        seen_keys = set()
        for r in route_list:
            if f"{r.relation}.{r.key}" in seen_keys:
                raise ValueError(
                    f"Duplicate route {r.relation}.{r.key} in trigger {name}."
                )

            r.validate(self)
            seen_keys.add(f"{r.relation}.{r.key}")
        self.route_map = {f"{r.relation}.{r.key}": r for r in self.routes()}

        # Set up params dictionary
        self._params = Params(self.name, params)

        # Set up initial state
        if len(inspect.signature(self.setUp).parameters) != 1:
            raise ValueError(
                f"setUp() of trigger {name} should have 1 argument"
            )

        self._state = {}
        self._version = version
        self._last_fit_id = -sys.maxsize - 1
        self.update(self.setUp(cursor))

        # Set up fit queue
        self._fit_queue = SimpleQueue()
        self._fit_thread = threading.Thread(
            target=self.processFitQueue,
            daemon=True,
            name=f"{name}_fit_thread",
        )
        self._fit_thread.start()

    @abstractmethod
    def routes(self):
        pass

    @abstractmethod
    def setUp(self, cursor):
        pass

    @property
    def params(self):
        return self._params

    @property
    def state(self):
        return self._state

    @property
    def version(self):
        return self._version

    @property
    def last_fit_id(self):
        return self._last_fit_id

    def update(self, new_state):
        if new_state:
            self._state.update(new_state)
            self._version += 1

    def processFitQueue(self):
        while True:
            (
                cursor,
                trigger_name,
                triggered_by,
                fit_event,
            ) = self._fit_queue.get()

            new_state = self.route_map.get(
                f"{triggered_by.relation}.{triggered_by.key}"
            ).fit(cursor, triggered_by)

            old_version = self.version
            self.update(new_state)

            logger.info(
                f"Finished running trigger {trigger_name} for identifier {triggered_by.identifier} and key {triggered_by.key}."
            )

            cursor.logTriggerExecution(
                trigger_name, old_version, "fit", triggered_by
            )

            fit_event.set()

    def fitWrapper(
        self,
        cursor,
        trigger_name,
        triggered_by: TriggerElement,
    ):
        fit_event = threading.Event()
        self._fit_queue.put((cursor, trigger_name, triggered_by, fit_event))

        return fit_event
