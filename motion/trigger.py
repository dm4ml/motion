import inspect
import logging
import threading
import typing

from abc import ABC, abstractmethod
from collections import namedtuple

TriggerElement = namedtuple("TriggerElement", ["namespace", "key", "value"])
TriggerFn = namedtuple("TriggerFn", ["name", "fn", "isTransform"])


class Trigger(ABC):
    def __init__(self, cursor, name, version):
        self._state = {}
        self._state_lock = threading.Lock()
        self._fit_lock = threading.Lock()
        self._version = version
        self.update(self.setUp(cursor))

        # Validate number of arguments in each trigger
        if len(inspect.signature(self.setUp).parameters) != 1:
            raise ValueError(
                f"setUp() of trigger {name} should have 1 argument"
            )

        if len(inspect.signature(self.shouldFit).parameters) != 3:
            raise ValueError(
                f"shouldFit() of trigger {name} should have 3 arguments"
            )

        if len(inspect.signature(self.fit).parameters) != 3:
            raise ValueError(
                f"fit() of trigger {name} should have 3 arguments"
            )

        if len(inspect.signature(self.shouldInfer).parameters) != 3:
            raise ValueError(
                f"shouldInfer() of trigger {name} should have 3 arguments"
            )

        if len(inspect.signature(self.infer).parameters) != 3:
            raise ValueError(
                f"infer() of trigger {name} should have 3 arguments"
            )

    @abstractmethod
    def setUp(self, cursor):
        pass

    @abstractmethod
    def shouldFit(self, cursor, id, triggered_by: TriggerElement):
        pass

    @abstractmethod
    def fit(self, cursor, id, triggered_by: TriggerElement):
        pass

    @abstractmethod
    def shouldInfer(self, cursor, id, triggered_by: TriggerElement):
        pass

    @abstractmethod
    def infer(self, cursor, id, triggered_by: TriggerElement):
        pass

    @property
    def state(self):
        return self._state

    @property
    def version(self):
        return self._version

    def update(self, new_state):
        if new_state:
            self._state.update(new_state)
            self._version += 1

    def fitAndLogAsync(
        self, cursor, trigger_name, id, triggered_by: TriggerElement
    ):
        with self._fit_lock:
            new_state = self.fit(cursor, id, triggered_by)
            with self._state_lock:
                old_version = self.version
                self.update(new_state)

        cursor.logTriggerExecution(
            trigger_name,
            old_version,
            "fit",
            triggered_by.namespace,
            id,
            triggered_by.key,
        )
        cursor.cur.close()
        logging.info(f"Finished running trigger {trigger_name} for id {id}.")

    def fitWrapper(
        self, cursor, trigger_name, id, triggered_by: TriggerElement
    ):
        thread = threading.Thread(
            target=self.fitAndLogAsync,
            args=(cursor, trigger_name, id, triggered_by),
        )
        thread.start()
