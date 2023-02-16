from graphlib import TopologicalSorter
from motion.executors.transform_exec import TransformExecutorV2
from rich.progress import track
from rich.tree import Tree
from rich import print as rprint
from rich.panel import Panel
from rich.console import Group
from rich.layout import Layout

import asyncio
import copy
import inspect
import dataclasses


class PipelineExecutorV2(object):
    def __init__(self, store):
        self.store = store
        self.transforms = {}
        self.transform_dag = {}

    def addTransform(self, transform, upstream=[]):
        self.transform_dag[transform.__name__] = {
            dep.__name__ for dep in upstream
        }
        self.transforms[transform.__name__] = TransformExecutorV2(
            transform,
            self.store,
            [
                self.transforms[dep]
                for dep in self.transform_dag[transform.__name__]
            ],
        )

    def printPipeline(self):
        ts = TopologicalSorter(self.transform_dag)
        ts.prepare()
        panels = []

        while ts.is_active():
            for node in ts.get_ready():
                features_tree = Tree(
                    "[underline yellow]Features",
                    guide_style="bold bright_blue",
                )
                if self.transforms[node].transform.featureType is not None:
                    for field in dataclasses.fields(
                        self.transforms[node].transform.featureType
                    ):
                        features_tree.add(field.name)
                labels_tree = Tree(
                    "[underline yellow]Labels", guide_style="bold bright_blue"
                )
                if self.transforms[node].transform.labelType is not None:
                    for field in dataclasses.fields(
                        self.transforms[node].transform.labelType
                    ):
                        labels_tree.add(field.name)

                panels.append(
                    Panel(
                        Group(features_tree, labels_tree),
                        border_style="bold bright_blue",
                        title=f"[bold yellow]{node}",
                        expand=False,
                    )
                )
                ts.done(node)

        layout = Layout()
        layout.size = None
        layout.split_row(*[Layout(p) for p in panels])
        rprint(layout)

    async def _executemany(self, ids):
        # TODO(shreyashankar): figure out how to handle caching (after parallelization)

        # Run topological sort
        ts = TopologicalSorter(self.transform_dag)
        final_node = list(ts.static_order())[-1]
        te = self.transforms[final_node]
        
        infer_calls = [te.infer(id, version=id) for id in ids]
        results = await asyncio.gather(*infer_calls)
        return {id: res for id, res in zip(ids, results)}
    
    def executemany(self, ids):
        return asyncio.run(self._executemany(ids))

    def executeone(self, id):
        return self.executemany([id])[id]
    
    def printStates(self):
        for node in self.transforms:
            rprint(node)
            rprint(self.transforms[node].state_history)