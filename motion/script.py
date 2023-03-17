from abc import ABC, abstractmethod


class MotionScript(ABC):
    def __init__(self, store):
        self._store = store

    @property
    def store(self):
        return self._store

    @abstractmethod
    def run(self):
        pass
