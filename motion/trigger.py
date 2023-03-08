import copy
import inspect
import typing

from abc import ABC, abstractmethod
from collections import namedtuple

TriggerElement = namedtuple("TriggerElement", ["namespace", "key", "value"])


class Trigger(ABC):
    def __init__(self, store):
        self._state = {}
        self._mode = "setUp"
        self.store = store
        self.setUp()

        # Check that shouldInfer and infer do not modify state
        if "self.setState" in inspect.getsource(self.shouldInfer):
            raise ValueError("shouldInfer should not modify state")

        if "self.setState" in inspect.getsource(self.infer):
            raise ValueError("infer should not modify state")

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
    def mode(self):
        return self._mode

    def setState(self, state):
        if self.mode not in ["setUp", "fit"]:
            raise RuntimeError("setState can only be called in setUp or fit.")

        self._state.update(state)

    def execute(self, id, triggered_by):
        self._mode = "shouldInfer"
        if self.shouldInfer(id, triggered_by):
            self._mode = "infer"
            self.infer(id, triggered_by)

        self._mode = "shouldFit"
        if self.shouldFit(id, triggered_by):
            self._mode = "fit"
            self.fit(id, triggered_by)
