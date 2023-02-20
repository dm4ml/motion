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
    def __init__(self, store, feedback_key="_feedback", output_key="_output"):
        self.store = store
        self.transforms = {}
        self.transform_dag = {}
        self.feedback_key = feedback_key
        self.output_key = output_key

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
                for field in self.transforms[node].feature_fields:
                    features_tree.add(field)
                labels_tree = Tree(
                    "[underline yellow]Labels", guide_style="bold bright_blue"
                )
                for field in self.transforms[node].label_fields:
                    labels_tree.add(field)
                return_tree = Tree(
                    "[underline yellow]Return", guide_style="bold bright_blue"
                )
                for field in self.transforms[node].return_fields:
                    return_tree.add(field)

                panels.append(
                    Panel(
                        Group(features_tree, labels_tree, return_tree),
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

    async def _executemany(self, ids, log=True):
        # TODO(shreyashankar): figure out how to handle caching (after parallelization)

        # Run topological sort
        ts = TopologicalSorter(self.transform_dag)
        final_node = list(ts.static_order())[-1]
        te = self.transforms[final_node]

        infer_calls = [te.infer(id, version=id, lazy=True) for id in ids]
        await asyncio.gather(*infer_calls)
        results = await te.processOutstanding()
        if log:
            for id in ids:
                self.store.set(id, self.output_key, results[id])

        return results

    def executemany(self, ids):
        return asyncio.run(self._executemany(ids))

    def executeone(self, id, log=True):
        res = self.executemany([id])
        if log:
            self.store.set(id, self.output_key, res[id])
        return res[id]

    def printStates(self):
        for node in self.transforms:
            rprint(node)
            rprint(self.transforms[node].state_history)

    def logFeedback(self, id, feedback):
        self.store.set(id, self.feedback_key, feedback)

    def logFeedbackMany(self, ids, feedbacks):
        for id, feedback in zip(ids, feedbacks):
            self.logFeedback(id, feedback)
