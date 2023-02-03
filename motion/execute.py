"""
Things we need to do:
* Execute computation
* Handle retraining
* Figure out how to display results
* Handle dependent models
"""
from graphlib import TopologicalSorter
from rich.progress import track
from rich.tree import Tree
from rich import print as rprint
from rich.panel import Panel
from rich.console import Group

import copy
import dataclasses


class TransformExecutor(object):
    def __init__(self, transform, store):
        # TODO(shreyashankar): Add to the buffer when inference is performed
        self.buffer = []
        self.state_history = {}
        self.step = None
        self.transform = transform(self)
        self.store = store
        self.max_staleness = (
            self.transform.max_staleness
            if hasattr(self.transform, "max_staleness")
            else 0
        )

    def versionState(self, state):
        if self.step is None:
            raise ValueError(
                "Cannot update state in a transform's constructor."
            )
        self.state_history[self.step] = copy.deepcopy(state)

    def fit(self, id):
        train_ids = self.store.idsBefore(id)
        features = []
        labels = []

        assert id not in train_ids

        for train_id in train_ids:
            feature_values = self.store.mget(
                train_id,
                [
                    field.name
                    for field in dataclasses.fields(self.transform.featureType)
                ],
            )
            features.append(
                self.transform.featureType(
                    **{
                        k: v
                        for k, v in feature_values.items()
                        if v is not None
                    }
                )
            )
            label_values = self.store.mget(
                train_id,
                [
                    field.name
                    for field in dataclasses.fields(self.transform.labelType)
                ],
            )
            labels.append(
                self.transform.labelType(
                    **{k: v for k, v in label_values.items() if v is not None}
                )
            )

        # Fit transform to training set
        self.transform._check_type(features, labels)
        self.step = id
        self.transform.fit(features, labels)

    def infer(self, id):
        # Retrieve features
        feature_values = self.store.mget(
            id,
            [
                field.name
                for field in dataclasses.fields(self.transform.featureType)
            ],
        )
        features = self.transform.featureType(
            **{k: v for k, v in feature_values.items() if v is not None}
        )

        # Type check features
        self.transform._check_type([features])

        # Find most recent state <= id
        version = max(
            [
                v
                for v in self.state_history.keys()
                if v <= id and v >= id - self.max_staleness
            ]
            or [None]
        )

        if not version:
            # Train on all data up to this point
            self.fit(id)
            version = id

        # Infer using the correct state
        old_state = self.transform.state
        self.transform.state = self.state_history[version]
        # print("Using version {0} for tuple {1}".format(version, id))
        result = self.transform.infer(features)
        self.transform.state = old_state
        return result


class PipelineExecutor(object):
    def __init__(self, store):
        self.store = store
        self.transforms = {}
        self.transform_dag = {}
        self.ts = None

    def addTransform(self, transform, dependencies=[]):
        self.transforms[transform.__name__] = TransformExecutor(
            transform, self.store
        )
        self.transform_dag[transform.__name__] = {
            dep.__name__ for dep in dependencies
        }

    def printPipeline(self):
        ts = TopologicalSorter(self.transform_dag)
        ts.prepare()

        while ts.is_active():
            for node in ts.get_ready():
                node_tree = Tree(
                    Panel.fit(
                        f"{node}",
                        style="bold bright_blue",
                    ),
                    guide_style="bold bright_blue",
                )
                features_tree = node_tree.add("[bold yellow]Features")
                for field in dataclasses.fields(
                    self.transforms[node].transform.featureType
                ):
                    features_tree.add(field.name)
                labels_tree = node_tree.add("[bold yellow]Labels")
                for field in dataclasses.fields(
                    self.transforms[node].transform.labelType
                ):
                    labels_tree.add(field.name)
                ts.done(node)

                rprint(node_tree)

    def executemany(self, ids):
        # TODO(shreyashankar): figure out how to handle dependent models
        # TODO(shreyashankar): figure out how to handle caching (after parallelization)

        # Run topological sort
        ts = TopologicalSorter(self.transform_dag)
        ts.prepare()
        results = {}

        while ts.is_active():
            for node in ts.get_ready():
                # Retrieve transform and do work for the ids
                te = self.transforms[node]

                for id in track(ids, description="Running the pipeline..."):
                    results[id] = te.infer(id)

                ts.done(node)

        return results

    def executeone(self, id):
        return self.executemany([id])[id]
