"""
Things we need to do:
* Execute computation
* Handle retraining
* Figure out how to display results
* Handle dependent models
"""
from concurrent import futures
from graphlib import TopologicalSorter
from rich.progress import track
from rich.tree import Tree
from rich import print as rprint
from rich.panel import Panel
from rich.console import Group
from rich.layout import Layout
from threading import Thread, Lock

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
        # self.step = None
        self.transform = transform(self)
        self.store = store
        self.upstream_executors = upstream_executors
        self.max_staleness = (
            self.transform.max_staleness
            if hasattr(self.transform, "max_staleness")
            else 0
        )

        # Set types
        self.feature_fields = [
            f.name for f in _get_fields_from_type(self.transform.featureType)
        ]
        if self.transform.labelType:
            self.label_fields = [
                f.name for f in _get_fields_from_type(self.transform.labelType)
            ]
        if self.transform.returnType and dataclasses.is_dataclass(
            self.transform.returnType
        ):
            self.return_fields = [
                f.name
                for f in _get_fields_from_type(self.transform.returnType)
            ]

        # Set locks
        self.state_history_lock = Lock()

    def versionState(self, step, state):
        # if self.step is None:
        #     raise ValueError(
        #         "Cannot update state in a transform's constructor."
        #     )
        self.state_history[step] = copy.deepcopy(state)

    def fetchFeatures(self, ids, version):
        # Get from datastore
        feature_values = {}
        for id in ids:
            feature_values[id] = self.store.mget(
                id,
                self.feature_fields,
            )

        # Replace from upstream if necessary
        for upstream in self.upstream_executors:
            upstream_feature_names = [
                f for f in upstream.return_fields if f in self.feature_fields
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

        return features

    def fit(self, id):
        # print(f"Requesting lock for id {id}")
        with self.state_history_lock:
            # print(f"Got lock for id {id}")
            train_ids = self.store.idsBefore(id)
            features = None
            labels = None

            assert id not in train_ids

            # features = []

            # Run fetchFeatures for all train_ids
            features = self.fetchFeatures(train_ids, version=id)

            if self.transform.labelType is not None:
                labels = []
                for train_id in train_ids:
                    label_values = self.store.mget(
                        train_id,
                        self.label_fields,
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

            new_state = self.transform.fit(features=features, labels=labels)
            self.transform.state = new_state
            self.versionState(id, new_state)
            # print(f"Releasing lock for id {id}")

    def infer(self, id, version=None):
        # Retrieve features
        features = self.fetchFeatures([id], version=id)[0]

        # Type check features
        self.transform._check_type(features=[features])

        # If version is specified, find the closest version <= version
        if version:
            with self.state_history_lock:
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
            with self.state_history_lock:
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
        # print(f"Requesting lock for inference on id {id}")
        with self.state_history_lock:
            # print(f"Acquired lock for inference on id {id}")
            correct_state = self.state_history[version]

        # TODO(shreyashankar): make this multiprocessed?
        def infer_helper(queue):
            return self.transform.infer(correct_state, features)

        result = self.transform.infer(correct_state, features)
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

    def executemany(self, ids, max_workers=1):
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

                with futures.ThreadPoolExecutor(
                    max_workers=max_workers
                ) as executor:
                    curr_results = {
                        executor.submit(te.infer, id): id for id in ids
                    }

                    # for id in track(ids, description=f"Executing {node}..."):
                    #     curr_results[id] = executor.submit(te.infer, id)

                    for future in futures.as_completed(curr_results):
                        id = curr_results[future]
                        try:
                            results[node][id] = future.result()
                        except Exception as e:
                            print(e)
                            raise e

                    ts.done(node)
                    last_node = node

        return results[last_node]

    def executeone(self, id):
        return self.executemany([id])[id]
