"""
Interface for connecting with the data source.

TODO(shreyashankar):
* Validate id uniqueness
* Maintain order and easily retrieve batches of tuples
"""

from abc import ABC, abstractmethod
from collections import OrderedDict
from sklearn import datasets

import json
import pandas as pd


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


class SklearnStore(Store):
    def __init__(self, name: str = "iris"):
        if name == "iris":
            data = datasets.load_iris()
            df = pd.DataFrame(data=data.data, columns=data.feature_names)
            df["target"] = data["target"]
            df.columns = df.columns.str.replace("(cm)", "", regex=False)
        elif name == "breast_cancer":
            data = datasets.load_breast_cancer()
            df = pd.DataFrame(data=data.data, columns=data.feature_names)
            df["target"] = data["target"]
        elif name == "covertype":
            data = datasets.fetch_covtype()
            df = pd.DataFrame(data=data.data, columns=data.feature_names)
            df["target"] = data["target"]
        else:
            raise ValueError(f"Unknown dataset {name}")

        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.replace(" ", "_")
        self.store = df.sample(frac=1).reset_index(drop=True).to_dict("index")

    def get(self, id, key):
        return self.store[id][key] if key in self.store[id] else None

    def mget(self, id, keys):
        return {
            key: self.store[id][key] for key in keys if key in self.store[id]
        }

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
        return key_list[: key_list.index(id)]


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
        return key_list[: key_list.index(id)]
