import copy
import typing

from abc import ABC, abstractmethod


class Transform(ABC):
    def __init__(self):
        self.buffer = []
        self.state_history = {}
        self.step = (
            None  # TODO(shreyashankar): figure out how to update the step
        )

    def _check_type(self, features, labels):
        if not isinstance(features[0], self.feature_type):
            raise TypeError(f"Features must be of type {self.feature_type}")
        if not isinstance(labels[0], self.label_type):
            raise TypeError(f"Labels must be of type {self.label_type}")

    def updateState(self, state):
        self.state.update(state)
        self.state_history[self.step] = copy.deepcopy(self.state)

    @abstractmethod
    def fit(self, features, labels):
        pass

    def inc(self, features, labels):  # Optional
        pass

    @abstractmethod
    def infer(self, features):
        pass

    def __call__(self, *args, **kwargs):
        return self.infer(*args, **kwargs)
