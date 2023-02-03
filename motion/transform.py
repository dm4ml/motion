import copy
import typing

from abc import ABC, abstractmethod


class Transform(ABC):
    def __init__(self, executor):
        self.executor = executor
        self.state = {}

    def _check_type(self, features, labels=None):
        if not isinstance(features[0], self.featureType):
            raise TypeError(f"Features must be of type {self.featureType}")
        if labels and not isinstance(labels[0], self.labelType):
            raise TypeError(f"Labels must be of type {self.labelType}")

    def updateState(self, state):
        self.state.update(state)
        self.executor.versionState(state)

    @abstractmethod
    def fit(self, features, labels) -> None:
        pass

    def inc(self, features, labels) -> None:  # Optional
        pass

    @abstractmethod
    def infer(self, features) -> typing.Any:
        pass

    def __call__(self, *args, **kwargs):
        return self.infer(*args, **kwargs)
