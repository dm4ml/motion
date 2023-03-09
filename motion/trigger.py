import copy
import inspect
import typing

from abc import ABC, abstractmethod
from collections import namedtuple

TriggerElement = namedtuple("TriggerElement", ["namespace", "key", "value"])
TriggerFn = namedtuple("TriggerFn", ["name", "fn", "isTransform"])


class Trigger(ABC):
    def __init__(self, cursor, name, version):
        self._state = {}
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
