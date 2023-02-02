"""
Interface for connecting with the data source.

TODO(shreyashankar):
* Validate id uniqueness
* Maintain order and easily retrieve batches of tuples
"""

from abc import ABC, abstractmethod
from collections import OrderedDict

import json


class Store(ABC):
    @abstractmethod
    def get(self, id, key):
        pass

    @abstractmethod
    def mget(self, id, keys):
        pass

    @abstractmethod
    def set(self, id, key, value):
        pass

    @abstractmethod
    def mset(self, id, key_values):
        pass


class JSONMemoryStore(Store):
    def __init__(self, filename: str):
        self.store = json.load(
            open(filename, "r"), object_pairs_hook=OrderedDict
        )

    def get(self, id, key):
        return self.store[id][key]

    def mget(self, id, keys):
        return {key: self.store[id][key] for key in keys}

    def set(self, id, key, value):
        if id not in self.store:
            self.store[id] = {}
        self.store[id][key] = value

    def mset(self, id, key_values):
        if id not in self.store:
            self.store[id] = {}
        self.store[id].update(key_values)

    def idsBefore(self, id):
        key_list = list(self.store.keys())
        return key_list[: key_list.indexof(id)]
