import copy
import inspect
import typing

from abc import ABC, abstractmethod
from collections import namedtuple

TriggerElement = namedtuple("TriggerElement", ["namespace", "key", "value"])


class Trigger(ABC):
    def __init__(self, store, name, version):
        self._state = {}
        self.name = name
        self.store = store
        self._version = version
        self.update(self.setUp())

    @abstractmethod
    def setUp(self):
        pass

    @abstractmethod
    def shouldFit(self, id, triggered_by: TriggerElement):
        pass

    @abstractmethod
    def fit(self, id, triggered_by: TriggerElement):
        pass

    @abstractmethod
    def shouldInfer(self, id, triggered_by: TriggerElement):
        pass

    @abstractmethod
    def infer(self, id, triggered_by: TriggerElement):
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

    def execute(self, id, triggered_by):
        if self.shouldInfer(id, triggered_by):
            self.infer(id, triggered_by)
            self.store.logTriggerExecution(
                self.name,
                self.version,
                "infer",
                triggered_by.namespace,
                id,
                triggered_by.key,
            )

        if self.shouldFit(id, triggered_by):
            # TODO(shreyashankar): Asynchronously trigger this
            new_state = self.fit(id, triggered_by)
            old_version = self.version
            self.update(new_state)
            self.store.logTriggerExecution(
                self.name,
                old_version,
                "fit",
                triggered_by.namespace,
                id,
                triggered_by.key,
            )
