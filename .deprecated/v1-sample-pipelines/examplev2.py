from dataclasses import dataclass

from motion.transform import Transform
from motion.data import SklearnStore
from motion.executors import PipelineExecutorV2

from rich import print, pretty
from sklearn import ensemble
from sklearn.preprocessing import MinMaxScaler

import numpy as np
import typing


@dataclass
class BreastFeature:
    mean_radius: float
    mean_texture: float
    mean_perimeter: float
    mean_area: float
    mean_smoothness: float
    mean_compactness: float
    mean_concavity: float
    mean_concave_points: float
    mean_symmetry: float
    mean_fractal_dimension: float
    radius_error: float
    texture_error: float
    perimeter_error: float
    area_error: float
    smoothness_error: float
    compactness_error: float
    concavity_error: float
    concave_points_error: float
    symmetry_error: float
    fractal_dimension_error: float
    worst_radius: float
    worst_texture: float
    worst_perimeter: float
    worst_area: float
    worst_smoothness: float
    worst_compactness: float
    worst_concavity: float
    worst_concave_points: float
    worst_symmetry: float
    worst_fractal_dimension: float

    def __array__(self) -> np.ndarray:
        return np.array(
            [getattr(self, field) for field in self.__dataclass_fields__]
        )


@dataclass
class BreastLabel:
    target: int


class Preprocess(Transform):
    featureType = BreastFeature
    labelType = None
    returnType = BreastFeature

    def setUp(self):
        self.max_staleness = 10

    # TODO(shreyashankar): get rid of labels if user doesn't want them
    def fit(
        self,
        features: typing.List[featureType],
        labels: typing.List[labelType],
    ):
        train_set = np.array([np.array(f) for f in features])
        scaler = MinMaxScaler()
        scaler.fit(train_set)
        return {"scaler": scaler}

    def infer(self, state, feature: featureType):
        return BreastFeature(
            *state["scaler"].transform(np.array(feature).reshape(1, -1))[
                0
            ]
        )


class Model(Transform):
    featureType = BreastFeature
    labelType = BreastLabel
    returnType = int

    def setUp(self):
        self.max_staleness = 10

    def fit(
        self,
        features: typing.List[featureType],
        labels: typing.List[labelType],
    ):
        model = ensemble.RandomForestClassifier()

        train_set = np.array([np.array(f) for f in features])
        train_target = np.array([l.target for l in labels])
        model.fit(train_set, train_target)

        train_acc = model.score(train_set, train_target)
        return {"model": model, "train_acc": train_acc}

    def infer(self, state, feature: featureType):
        return state["model"].predict(np.array(feature).reshape(1, -1))[0]


if __name__ == "__main__":
    pretty.install()

    # Create a store
    store = SklearnStore("breast_cancer")
    test_ids = [
        int(elem)
        for elem in np.arange(0.8 * len(store.store), len(store.store))
    ]

    pe = PipelineExecutorV2(store)
    # pe.addTransform(Model)
    pe.addTransform(Preprocess)
    pe.addTransform(Model, [Preprocess])

    # Print pipeline
    pe.printPipeline()

    # Execute
    preds = pe.executemany(test_ids)

    # Compute accuracy
    numerator = 0
    denominator = 0
    for test_id in test_ids:
        true = store.get(test_id, "target")
        if true == preds[test_id]:
            numerator += 1
        denominator += 1
    print("Accuracy: {0:.4f}".format(float(numerator / denominator)))

    # Try to execute some old id
    old_id = 15
    old_pred = pe.executeone(old_id)
    print(
        "Old prediction and label for id {0}: {1}, {2}".format(
            old_id, old_pred, store.get(old_id, "target")
        )
    )

    # Try to execute some newer id we've already executed
    id2 = 125
    pred2 = pe.executeone(id2)
    print(
        "Prediction and label for id {0}: {1}, {2}".format(
            id2, pred2, store.get(id2, "target")
        )
    )

    # Try to execute some middle id
    id3 = 70
    pred3 = pe.executeone(id3)
    print(
        "Prediction and label for id {0}: {1}, {2}".format(
            id3, pred3, store.get(id3, "target")
        )
    )

    # Print state
    for t in pe.transforms.keys():
        print(t)
        print(pe.transforms[t].state_history)