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
from rich.layout import Layout

import copy
import inspect
import dataclasses


def _get_fields_from_type(t):
    if not t:
        raise ValueError("Cannot get fields from None type.")
    return dataclasses.fields(t)


class TransformExecutor(object):
    def __init__(self, transform, store, upstream_executors=[]):
        # TODO(shreyashankar): Add to the buffer when inference is performed
        self.buffer = []
        self.state_history = {}
        self.step = None
        self.transform = transform(self)
        self.store = store
        self.upstream_executors = upstream_executors
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

    def fetchFeatures(self, ids, version):
        if self.transform.featureType is None:
            return None

        # Get from datastore
        feature_fields = _get_fields_from_type(self.transform.featureType)
        feature_values = {}
        for id in ids:
            feature_values[id] = self.store.mget(
                id,
                [field.name for field in feature_fields],
            )

        # Replace from upstream if necessary
        for upstream in self.upstream_executors:
            upstream_feature_names = [
                field.name
                for field in _get_fields_from_type(
                    upstream.transform.returnType
                )
                if field in feature_fields
            ]
            for id in ids:
                upstream_features = upstream.infer(
                    id, version=version
                ).__dict__
                feature_values[id].update(
                    {
                        k: v
                        for k, v in upstream_features.items()
                        if k in upstream_feature_names
                    }
                )

        features = [
            self.transform.featureType(
                **{
                    k: v
                    for k, v in feature_values[id].items()
                    if v is not None
                }
            )
            for id in ids
        ]

        # if self.upstream_executors:
        #     rprint(features)

        return features

    def fit(self, id):
        train_ids = self.store.idsBefore(id)
        features = None
        labels = None

        assert id not in train_ids

        if self.transform.featureType is not None:
            # features = []

            # Run fetchFeatures for all train_ids
            features = self.fetchFeatures(train_ids, version=id)

            # for train_id in train_ids:
            #     features.append(self.fetchFeatures(train_id, version=id))
        if self.transform.labelType is not None:
            labels = []
            for train_id in train_ids:
                label_values = self.store.mget(
                    train_id,
                    [
                        field.name
                        for field in _get_fields_from_type(
                            self.transform.labelType
                        )
                    ],
                )
                labels.append(
                    self.transform.labelType(
                        **{
                            k: v
                            for k, v in label_values.items()
                            if v is not None
                        }
                    )
                )

        # Fit transform to training set
        self.transform._check_type(features=features, labels=labels)
        self.step = id
        self.transform.fit(features=features, labels=labels)

    def infer(self, id, version=None):
        # Retrieve features
        features = self.fetchFeatures([id], version=id)[0]

        # Type check features
        self.transform._check_type(features=[features])

        # If version is specified, find the closest version <= version
        if version:
            closest_version = max(
                [
                    v
                    for v in self.state_history.keys()
                    if v <= version and v > version - self.max_staleness
                ]
                or [None]
            )
            if closest_version:
                version = closest_version
            else:
                # Train on all data up to not including the version
                self.fit(version)

        # Find most recent state <= id if explicit version wasn't passed in
        if not version:
            version = max(
                [
                    v
                    for v in self.state_history.keys()
                    if v <= id and v > id - self.max_staleness
                ]
                or [None]
            )

            if not version:
                # Train on all data up to not including this point
                self.fit(id)
                version = id

            assert version <= id

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

    def addTransform(self, transform, upstream=[]):
        self.transform_dag[transform.__name__] = {
            dep.__name__ for dep in upstream
        }
        self.transforms[transform.__name__] = TransformExecutor(
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
                # node_tree = Tree(
                #     f"[bold yellow]{node}",
                #     guide_style="bold bright_blue",
                # )
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

    def executemany(self, ids):
        # TODO(shreyashankar): figure out how to handle caching (after parallelization)

        # Run topological sort
        ts = TopologicalSorter(self.transform_dag)
        ts.prepare()
        results = {}
        last_node = None

        while ts.is_active():
            for node in ts.get_ready():
                results[node] = {}

                # Retrieve transform and do work for the ids
                te = self.transforms[node]

                for id in track(ids, description=f"Executing {node}..."):
                    results[node][id] = te.infer(id)

                ts.done(node)
                last_node = node

        return results[last_node]

    def executeone(self, id):
        return self.executemany([id])[id]
