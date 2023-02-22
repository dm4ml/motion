from dataclasses import dataclass

from motion.transform import Transform
from motion.data import JSONMemoryStore
from motion.execute import PipelineExecutor


@dataclass
class FeatureType:
    unit_price: float
    quantity_on_hand: int = 0


@dataclass
class LabelType:
    indicator: int


class ScratchModel(Transform):
    featureType = FeatureType
    labelType = LabelType

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


if __name__ == "__main__":
    # Create a store
    store = JSONMemoryStore("fake_data.json")
    pe = PipelineExecutor(store)
    pe.addTransform(ScratchModel)

    # Execute
    preds = pe.execute(["079", "080", "081"])
    print(preds)

    # Print state
    print(pe.transforms["ScratchModel"].state_history)
