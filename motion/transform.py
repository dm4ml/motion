import copy
import typing

from abc import ABC, abstractmethod


class Transform(ABC):
    def __init__(self):
        self.setUp()

    @abstractmethod
    def setUp(self, store):
        pass

    @abstractmethod
    def shouldFit(self, new_id, triggered_by):
        pass

    @abstractmethod
    def fit(self, id):
        pass

    @abstractmethod
    def transform(self, id, triggered_by):
        pass
