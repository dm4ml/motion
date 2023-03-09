import copy
import inspect
import typing

from abc import ABC, abstractmethod
from collections import namedtuple

TriggerElement = namedtuple("TriggerElement", ["namespace", "key", "value"])


class Trigger(ABC):
    def __init__(self, store):
        self._state = {}
        self.store = store
        self._state.update(self.setUp())

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

    def execute(self, id, triggered_by):
        if self.shouldInfer(id, triggered_by):
            self.infer(id, triggered_by)

        if self.shouldFit(id, triggered_by):
            # TODO(shreyashankar): Asynchronously trigger this
            new_state = self.fit(id, triggered_by)
            self._state.update(new_state)
