"""
Things we need to do:
* Connect to database / data store
* Entrypoint for execution
* Execute computation
* Figure out how to display results
"""
from motion.data import JSONMemoryStore


class Executor(object):
    def __init__(self, data):
        self.data = data

    def execute(self, ids):
        pass
