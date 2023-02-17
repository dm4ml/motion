import copy
import typing

from abc import ABC, abstractmethod


class Transform(ABC):
    featureType = NotImplemented
    labelType = NotImplemented
    returnType = NotImplemented

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        if cls.featureType is NotImplemented:
            raise NotImplementedError(
                "Transforms must define a featureType class attribute."
            )
        if cls.labelType is NotImplemented:
            raise NotImplementedError(
                "Transforms must define a labelType class attribute."
            )
        if cls.returnType is NotImplemented:
            raise NotImplementedError(
                "Transforms must define a returnType class attribute."
            )

    def __init__(self, executor):
        self.executor = executor
        self.setUp()

    def setUp(self):
        pass

    def _check_type(self, **kwargs):
        features = kwargs.get("features", None)
        labels = kwargs.get("labels", None)
        if features and not isinstance(features[0], self.featureType):
            raise TypeError(f"Features must be of type {self.featureType}")
        if labels and not isinstance(labels[0], self.labelType):
            raise TypeError(f"Labels must be of type {self.labelType}")

    # def updateState(self, state):
    #     self.state.update(state)
    #     self.executor.versionState(state)

    # @abstractmethod
    def fit(self, **kwargs) -> typing.Dict:
        pass

    def inc(
        self, features, labels
    ) -> None:  # Optional (TODO shreyashankar: implement this when we implement the buffer)
        pass

    @abstractmethod
    def infer(self, feature) -> typing.Any:
        pass

    def __call__(self, *args, **kwargs):
        return self.infer(*args, **kwargs)

class TransformV2(ABC):
    # featureType = NotImplemented
    # labelType = NotImplemented
    # returnType = NotImplemented

    # def __init_subclass__(cls, **kwargs) -> None:
    #     super().__init_subclass__(**kwargs)
    #     if cls.featureType is NotImplemented:
    #         raise NotImplementedError(
    #             "Transforms must define a featureType class attribute."
    #         )
    #     if cls.labelType is NotImplemented:
    #         raise NotImplementedError(
    #             "Transforms must define a labelType class attribute."
    #         )
    #     if cls.returnType is NotImplemented:
    #         raise NotImplementedError(
    #             "Transforms must define a returnType class attribute."
    #         )

    def __init__(self):
        self.featureType = NotImplemented
        self.labelType = NotImplemented
        self.returnType = NotImplemented
        self.setUp()
        if self.featureType is NotImplemented:
            raise NotImplementedError(
                "Transforms must define a featureType class attribute."
            )
        if self.labelType is NotImplemented:
            raise NotImplementedError(
                "Transforms must define a labelType class attribute."
            )
        if self.returnType is NotImplemented:
            raise NotImplementedError(
                "Transforms must define a returnType class attribute."
            )

    def setUp(self):
        pass

    def _check_type(self, **kwargs):
        features = kwargs.get("features", None)
        labels = kwargs.get("labels", None)
        if features and not isinstance(features[0], self.featureType):
            raise TypeError(f"Features must be of type {self.featureType}")
        if labels and not isinstance(labels[0], self.labelType):
            raise TypeError(f"Labels must be of type {self.labelType}")

    # @abstractmethod
    def fit(self, **kwargs) -> typing.Dict:
        pass

    def inc(
        self, features, labels
    ) -> None:  # Optional (TODO shreyashankar: implement this when we implement the buffer)
        pass

    @abstractmethod
    def infer(self, feature) -> typing.Any:
        pass
    
    def inferBatch(self, features) -> typing.Any:
        pass

    def __call__(self, *args, **kwargs):
        return self.infer(*args, **kwargs)
