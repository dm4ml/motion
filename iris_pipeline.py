from dataclasses import dataclass

from motion.transform import Transform
from motion.data import SklearnStore
from motion.execute import PipelineExecutor

from rich import print, pretty
from sklearn import svm

import numpy as np


@dataclass
class IrisFeature:
    sepal_length: float
    sepal_width: float
    petal_length: float
    petal_width: float

    def __array__(self) -> np.ndarray:
        return np.array(
            [
                self.sepal_length,
                self.sepal_width,
                self.petal_length,
                self.petal_width,
            ]
        )


@dataclass
class IrisLabel:
    target: int


class SVM(Transform):
    featureType = IrisFeature
    labelType = IrisLabel

    def fit(self, features, labels):
        model = svm.SVC(kernel="linear", probability=True)

        train_set = np.array([np.array(f) for f in features])
        train_target = np.array([l.target for l in labels])
        model.fit(train_set, train_target)

        train_acc = model.score(train_set, train_target)
        self.updateState({"model": model, "train_acc": train_acc})

    def infer(self, features):
        return self.state["model"].predict(np.array(features).reshape(1, -1))[
            0
        ]


if __name__ == "__main__":
    pretty.install()

    # Create a store
    store = SklearnStore("iris")
    test_ids = [
        int(elem)
        for elem in np.arange(0.8 * len(store.store), len(store.store))
    ]

    pe = PipelineExecutor(store)
    pe.addTransform(SVM)

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
    print(pe.transforms["SVM"].state_history)
