import copy
import typing

from abc import ABC, abstractmethod
from collections import namedtuple

TriggerElement = namedtuple("TriggerElement", ["namespace", "key"])


class Transform(ABC):
    def __init__(self, store):
        self.setUp(store)

    @abstractmethod
    def setUp(self, store):
        pass

    @abstractmethod
    def shouldFit(self, new_id, triggered_by: TriggerElement):
        pass

    @abstractmethod
    def fit(self, id):
        pass

    @abstractmethod
    def transform(self, id, triggered_by: TriggerElement):
        pass
