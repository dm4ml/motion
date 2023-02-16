from dataclasses import dataclass

from motion.transform import Transform
from motion.data import SklearnStore
from motion.execute import PipelineExecutor
from motion.executors import PipelineExecutorV2

from rich import print, pretty
from sklearn import ensemble
from sklearn.preprocessing import MinMaxScaler

import cProfile
import numpy as np
import typing


@dataclass
class CovertypeFeature:
    Elevation: float
    Aspect: float
    Slope: float
    Horizontal_Distance_To_Hydrology: float
    Vertical_Distance_To_Hydrology: float
    Horizontal_Distance_To_Roadways: float
    Hillshade_9am: float
    Hillshade_Noon: float
    Hillshade_3pm: float
    Horizontal_Distance_To_Fire_Points: float
    Wilderness_Area_0: float
    Wilderness_Area_1: float
    Wilderness_Area_2: float
    Wilderness_Area_3: float
    Soil_Type_0: float
    Soil_Type_1: float
    Soil_Type_2: float
    Soil_Type_3: float
    Soil_Type_4: float
    Soil_Type_5: float
    Soil_Type_6: float
    Soil_Type_7: float
    Soil_Type_8: float
    Soil_Type_9: float
    Soil_Type_10: float
    Soil_Type_11: float
    Soil_Type_12: float
    Soil_Type_13: float
    Soil_Type_14: float
    Soil_Type_15: float
    Soil_Type_16: float
    Soil_Type_17: float
    Soil_Type_18: float
    Soil_Type_19: float
    Soil_Type_20: float
    Soil_Type_21: float
    Soil_Type_22: float
    Soil_Type_23: float
    Soil_Type_24: float
    Soil_Type_25: float
    Soil_Type_26: float
    Soil_Type_27: float
    Soil_Type_28: float
    Soil_Type_29: float
    Soil_Type_30: float
    Soil_Type_31: float
    Soil_Type_32: float
    Soil_Type_33: float
    Soil_Type_34: float
    Soil_Type_35: float
    Soil_Type_36: float
    Soil_Type_37: float
    Soil_Type_38: float
    Soil_Type_39: float

    def __array__(self) -> np.ndarray:
        return np.array(
            [getattr(self, field) for field in self.__dataclass_fields__]
        )


@dataclass
class CovertypeLabel:
    target: int


class Preprocess(Transform):
    featureType = CovertypeFeature
    labelType = None
    returnType = CovertypeFeature

    def setUp(self):
        self.max_staleness = 1e6
        self.min_train_size = 100

    def fit(
        self,
        features: typing.List[featureType],
        labels: typing.List[labelType],
    ):
        scaler = MinMaxScaler()
        train_set = np.array([np.array(f) for f in features])
        scaler.fit(train_set)
        return {"scaler": scaler}

    def infer(self, state, feature: featureType) -> typing.Any:
        return CovertypeFeature(
            *state["scaler"].transform(np.array(feature).reshape(1, -1))[0]
        )


class Model(Transform):
    featureType = CovertypeFeature
    labelType = CovertypeLabel
    returnType = CovertypeLabel

    def setUp(self):
        self.max_staleness = 1e6
        self.min_train_size = 100

    def fit(
        self,
        features: typing.List[featureType],
        labels: typing.List[labelType],
    ):
        model = ensemble.RandomForestClassifier(random_state=0)

        train_set = np.array([np.array(f) for f in features])
        train_target = np.array([l.target for l in labels])
        model.fit(train_set, train_target)

        train_acc = model.score(train_set, train_target)
        return {"model": model, "train_acc": train_acc}

    def infer(self, state, feature: featureType):
        return CovertypeLabel(
            target=int(
                state["model"].predict(np.array(feature).reshape(1, -1))[0]
            )
        ).target


class Identity(Transform):
    featureType = CovertypeLabel
    labelType = None
    returnType = int

    def setUp(self):
        self.ignore_fit = True

    def infer(self, state, feature):
        return feature.target * 1


if __name__ == "__main__":
    pretty.install()

    # Create a store
    store = SklearnStore("covertype")
    test_ids = [
        int(elem)
        for elem in np.arange(0.8 * len(store.store), len(store.store))
    ]

    pe = PipelineExecutorV2(store)
    # pe = PipelineExecutor(store)
    # pe.addTransform(Model)
    pe.addTransform(Preprocess)
    pe.addTransform(Model, [Preprocess])
    # pe.addTransform(Identity, [Model])

    # Print pipeline
    pe.printPipeline()

    # Execute
    test_ids = [int(elem) for elem in range(1000, 2000)]
    preds = None
    cProfile.run("preds = pe.executemany(test_ids)") #727.509 seconds for v2 but 0 accuracy? 443.327 seconds for v1 but also 0 accuracy

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
    # old_id = 15
    # old_pred = pe.executeone(old_id)
    # print(
    #     "Old prediction and label for id {0}: {1}, {2}".format(
    #         old_id, old_pred, store.get(old_id, "target")
    #     )
    # )

    # # Try to execute some newer id we've already executed
    # id2 = 125
    # pred2 = pe.executeone(id2)
    # print(
    #     "Prediction and label for id {0}: {1}, {2}".format(
    #         id2, pred2, store.get(id2, "target")
    #     )
    # )

    # # Try to execute some middle id
    # id3 = 70
    # pred3 = pe.executeone(id3)
    # print(
    #     "Prediction and label for id {0}: {1}, {2}".format(
    #         id3, pred3, store.get(id3, "target")
    #     )
    # )

    # Print state
    for t in pe.transforms.keys():
        print(t)
        print(pe.transforms[t].state_history)
