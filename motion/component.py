from abc import ABC, abstractmethod
from typing import Any, Dict

from motion.execute import CustomDict, Executor


class Component(ABC):
    def __init__(self, params: Dict[str, Any] = {}):
        self._executor = Executor(self)
        self._params = CustomDict(self.__class__.__name__, "params", params)

    @property
    def params(self) -> CustomDict:
        return self._params

    @abstractmethod
    def setUp(self) -> Dict[str, Any]:
        pass

    @staticmethod
    def infer(key: str) -> Any:
        def decorator(func: Any) -> Any:
            func._input_key = key
            func._op = "infer"
            return func

        return decorator

    @staticmethod
    def fit(key: str, batch_size: int = 1) -> Any:
        def decorator(func: Any) -> Any:
            func._input_key = key
            func._batch_size = batch_size
            func._op = "fit"
            return func

        return decorator

    def run(self, **kwargs: Any) -> Any:
        return_fit_event = kwargs.pop("return_fit_event", False)
        wait_for_fit = kwargs.pop("wait_for_fit", False)

        infer_result, fit_event = self._executor.run(**kwargs)

        if wait_for_fit:
            if not fit_event:
                raise ValueError(
                    "wait_for_fit is True, but no there is no fit event for this route."
                )
            fit_event.wait()

        if return_fit_event:
            return infer_result, fit_event

        return infer_result
