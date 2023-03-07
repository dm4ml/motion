import copy
import inspect
import typing

from abc import ABC, abstractmethod
from collections import namedtuple

TriggerElement = namedtuple("TriggerElement", ["namespace", "key", "value"])


class Transform(ABC):
    def __init__(self, store):
        self._state = {}
        self.setUp(store)

        # Check that shouldInfer and infer do not modify state
        if "self.setState" in inspect.getsource(self.shouldInfer):
            raise ValueError("shouldInfer should not modify state")

        if "self.setState" in inspect.getsource(self.infer):
            raise ValueError("infer should not modify state")

    @abstractmethod
    def setUp(self, store):
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

    def setState(self, state):
        self._state.update(state)
