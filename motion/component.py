import atexit
import inspect
import logging
from abc import ABC
from typing import Any, Dict, get_type_hints

from motion.execute import Executor
from motion.utils import CustomDict, configureLogging, logger, validate_args


def is_logger_open(logger: logging.Logger) -> bool:
    for handler in logger.handlers:
        if (
            hasattr(handler, "stream")
            and handler.stream is not None
            and not handler.stream.closed
        ):
            return True
    return False


class Component(ABC):
    def __init__(
        self,
        name: str,
        params: Dict[str, Any] = {},
        cleanup: bool = False,
        logging_level: str = "INFO",
    ):
        self._name = name
        self._executor = Executor(name, cleanup=cleanup)
        self._params = CustomDict(name, "params", params)
        configureLogging(logging_level)

        atexit.register(self.shutdown)

    def shutdown(self) -> None:
        is_open = is_logger_open(logger)

        if is_open:
            logger.info(f"Shutting down {self._name}...")

        self._executor.shutdown(is_open=is_open)

        if is_open:
            logger.info(f"Saving state from {self._name}...")
            # TODO: Save state
            logger.warning("State saving not implemented yet.")
            logger.info(f"Finished shutting down {self._name}.")

    @property
    def name(self) -> str:
        return self._name

    @property
    def params(self) -> CustomDict:
        return self._params

    def setUp(self, func: Any) -> Any:
        self._executor.init_state_func = func
        return func

    def infer(self, key: str) -> Any:
        def decorator(func: Any) -> Any:
            type_hint = get_type_hints(func).get("value", None)
            if not validate_args(inspect.signature(func).parameters, "infer"):
                raise ValueError(
                    f"Infer function {func.__name__} should have 2 arguments "
                    + "`state` and `value`"
                )

            def wrapper(state, value):
                if type_hint and not isinstance(value, type_hint):
                    try:
                        value = type_hint(**value)
                    except (TypeError, ValueError):
                        raise ValueError(
                            f"value argument must be of type {type_hint.__name__}"
                        )

                return func(state, value)

            wrapper._input_key = key
            wrapper._op = "infer"
            self._executor.add_route(wrapper._input_key, wrapper._op, wrapper)
            return wrapper

        return decorator

    def fit(self, key: str, batch_size: int = 1) -> Any:
        def decorator(func: Any) -> Any:
            if not validate_args(inspect.signature(func).parameters, "fit"):
                raise ValueError(
                    f"Fit method {func.__name__} should have 3 arguments: "
                    + "`state`, `values`, and `infer_results`."
                )

            func._input_key = key
            func._batch_size = batch_size
            func._op = "fit"
            self._executor.add_route(func._input_key, func._op, func)
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
