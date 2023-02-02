from dataclasses import dataclass

from motion.transform import Transform


@dataclass
class FeatureType:
    unit_price: float
    quantity_on_hand: int = 0


@dataclass
class LabelType:
    indicator: bool


class ScratchModel(Transform):
    featureType = FeatureType
    labelType = LabelType

    def __init__(self):
        self.state = {"model": None, "train_acc": None}
        super().__init__()

    def fit(self, features, labels):
        prices = [f.unit_price for f in features]
        average_price = sum(prices) / len(prices)
        model = lambda x: x.unit_price > average_price
        train_acc = sum(
            [model(f) == l.indicator for f, l in zip(features, labels)]
        ) / len(features)
        self.updateState({"model": model, "train_acc": train_acc})

    def infer(self, features):
        return self.state["model"](features)
