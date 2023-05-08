import atexit
import inspect
import logging
from typing import Any, Callable, Dict, Tuple, Union, get_type_hints

from motion.execute import Executor
from motion.utils import (
    CustomDict,
    FitEventGroup,
    configureLogging,
    logger,
    validate_args,
)


def is_logger_open(logger: logging.Logger) -> bool:
    for handler in logger.handlers:
        if (
            hasattr(handler, "stream")
            and handler.stream is not None
            and not handler.stream.closed
        ):
            return True
    return False


class Component:
    """Component class for creating Motion components.

    ## Examples

    === "Basic"
        ```python
        from motion import Component

        c = Component("MyAdder")

        @c.init
        def setUp():
            return {"value": 0}

        @c.infer("add")
        def plus(state, value):
            return state["value"] + value

        @c.fit("add")
        def add(state, values, infer_results):
            return {"value": state["value"] + sum(values)}

        if __name__ == "__main__":
            c.run(add=1, wait_for_fit=True) # Will return 1, blocking until fit
            # is done. Resulting state is {"value": 1}
            c.run(add=2) # Will return 3, not waiting for fit operation.
            # Resulting state will eventually be {"value": 3}
        ```

    === "Multiple Dataflows"
        ```python
        from motion import Component

        c = Component("Calculator")

        @c.init
        def setUp():
            return {"value": 0}

        @c.infer("add")
        def plus(state, value):
            return state["value"] + value

        @c.fit("add")
        def increment(state, values, infer_results):
            return {"value": state["value"] + sum(values)}

        @c.infer("subtract")
        def minus(state, value):
            return state["value"] - value

        @c.fit("subtract")
        def decrement(state, values, infer_results):
            return {"value": state["value"] - sum(values)}

        if __name__ == "__main__":
            c.run(add=1, wait_for_fit=True) # Will return 1, blocking until fit
            # is done. Resulting state is {"value": 1}
            c.run(subtract=1, wait_for_fit=True) # Will return 0, blocking
            # until fit is done. Resulting state is {"value": 0}
        ```

    === "Batch Size > 1"

        ```python
        from motion import Component
        import numpy as np

        c = Component("MLMonitor")

        @c.init
        def setUp():
            return {"model": YOUR_MODEL_HERE, "history": []}

        @c.infer("features")
        def predict(state, value):
            return state["model"].predict(value)

        @c.fit("features", batch_size=10)
        def monitor(state, values, infer_results):
            new_X = np.array(values)
            new_y = np.array(infer_results)
            concatenated = np.concatenate((state["history"], new_y))
            if YOUR_ANOMALY_ALGORITHM(concatenated, history):
                # Fire an alert
                YOUR_ALERT_FUNCTION()
            return {"history": history + [concatenated]}

        if __name__ == "__main__":
            c.run(features=YOUR_FEATURES_HERE) # Don't wait for fit to finish
            # because batch size is 10

            for _ in range(100):
                c.run(features=YOUR_FEATURES_HERE)
                # Some alert may be fired in the background!
        ```

    === "Type Validation"
        ```python
        from motion import Component
        from pydantic import BaseModel

        class MyModel(BaseModel):
            value: int

        c = Component("MyComponentWithValidation")

        @c.infer("noop")
        def noop(state, value: MyModel):
            return value.value

        if __name__ == "__main__":
            c.run(noop=MyModel(value=1)) # Will return 1
            c.run(noop={"value": 1}) # Will return 1
            c.run(noop=MyModel(value="1")) # Will raise an Error
            c.run(noop=1) # Will raise an Error
        ```
    """

    def __init__(
        self,
        name: str,
        params: Dict[str, Any] = {},
        cleanup: bool = False,
        logging_level: str = "INFO",
    ):
        """Creates a new Motion component.

        Args:
            name (str):
                Name of the component.
            params (Dict[str, Any], optional):
                Parameters to be accessed by the component. Defaults to {}.
                Usage: `c.params["param_name"]` if c is the component instance.
            cleanup (bool, optional):
                Whether to process the remainder of fit events after the user
                shuts down the program. Defaults to False.
            logging_level (str, optional):
                Logging level for the Motion logger. Uses the logging library.
                Defaults to "INFO".
        """
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
        """Name of the component.

        Example Usage:
        ```python
        from motion import Component

        c = Component("MyComponent")
        print(c.name) # Prints "MyComponent"
        ```

        Returns:
            str: Component name.
        """
        return self._name

    @property
    def params(self) -> Dict[str, Any]:
        """Parameters to use in component functions.

        Example Usage:
        ```python
        from motion import Component

        c = Component("MyComponent", params={"param1": 1, "param2": 2})

        @c.init
        def setUp():
            return {"value": 0}

        @c.infer("add")
        def plus(state, value):
            # Access params with c.params["param_name"]
            return state["value"] + value + c.params["param1"] + c.params
            ["param2"]
        ```

        Returns:
            Dict[str, Any]: Parameters dictionary.
        """
        return self._params

    def init(self, func: Callable) -> Callable:
        """Decorator for the init function. This function
        is called once at the beginning of the component's lifecycle.
        The decorated function should return a dictionary that represents
        the initial state of the component.

        Usage:
        ```python
        from motion import Component

        c = Component("MyComponent")

        @c.init
        def setUp():
            return {"value": 0}
        ```

        Args:
            func (Callable): Function without any arguments.

        Returns:
            Callable: Decorated init function.
        """
        # Assert that init function has no arguments
        if inspect.signature(func).parameters:
            raise ValueError("init function should have no arguments")
        self._executor.init_state_func = func
        return func

    def infer(self, key: str) -> Callable:
        """Decorator for any infer dataflow through the component. Takes
        in a string that represents the input keyword for the infer dataflow.

        2 arguments required for an infer function:
            * `state`: The current state of the component, which is a
                dictionary with string keys and any type values.
            * `value`: The value passed in through a `c.run` call with the
                `key` argument.

        Components can have multiple infer ops, but each infer op must have its
        own unique `key` argument. Infer ops should not modify the state
        object. If you want to modify the state object, use the `fit` decorator.

        The `value` argument can be optionally type checked with Pydantic type
        hints. If the type hint is a Pydantic model, the `value` argument will
        be converted to that model if it is a dictionary and not already of the
        model type.

        Example Usage:
        ```python
        from motion import Component

        c = Component("MyComponent")

        @c.init
        def setUp():
            return {"value": 0}

        @c.infer("add")
        def add(state, value):
            return state["value"] + value

        @c.infer("multiply")
        def multiply(state, value):
            return state["value"] * value

        c.run(add=1, wait_for_fit=True) # Returns 1
        c.run(multiply=2) # Returns 2
        ```

        Args:
            key (str): Keyword for the infer dataflow.

        Returns:
            Callable: Decorated infer function.
        """

        def decorator(func: Callable) -> Any:
            type_hint = get_type_hints(func).get("value", None)
            if not validate_args(inspect.signature(func).parameters, "infer"):
                raise ValueError(
                    f"Infer function {func.__name__} should have 2 arguments "
                    + "`state` and `value`"
                )

            def wrapper(state: CustomDict, value: Any) -> Any:
                if type_hint and not isinstance(value, type_hint):
                    try:
                        value = type_hint(**value)
                    except Exception:
                        raise ValueError(
                            f"value argument must be of type {type_hint.__name__}"
                        )

                return func(state, value)

            wrapper._input_key = key  # type: ignore
            wrapper._op = "infer"  # type: ignore
            self._executor.add_route(
                wrapper._input_key, wrapper._op, wrapper  # type: ignore
            )  # type: ignore
            return wrapper

        return decorator

    def fit(self, key: str, batch_size: int = 1) -> Any:
        """Decorator for any fit dataflows through the component. Takes
        in a string that represents the input keyword for the fit op.
        Only executes the fit op (function) when the batch size is reached.

        3 arguments required for a fit function:
            - `state`: The current state of the component, represented as a
            dictionary.
            - `values`: A list of values passed in through a `c.run` call with
            the `key` argument. Of length `batch_size`.
            - `infer_results`: A list of the results from the infer ops that
            correspond to the values in the `values` argument. Of length
            `batch_size`.

        Components can have multiple fit ops, and the same key can also have
        multiple fit ops. Fit functions should return a dictionary
        of state updates to be merged with the current state.

        Example Usage:
        ```python
        from motion import Component

        c = Component("MyComponent")

        @c.init
        def setUp():
            return {"value": 0}

        @c.fit("add")
        def add(state, values):
            return {"value": state["value"] + sum(values)}

        @c.infer("multiply")
        def multiply(state, value):
            return state["value"] * value

        @c.fit("multiply", batch_size=2) # Executes after 2 calls to c.run
        def multiply(state, values, infer_results):
            product = 1
            for value in values:
                product *= value
            return state["value"] * product

        c.run(add=1, wait_for_fit=True) # Returns 1
        c.run(multiply=2) # Returns 2, fit not executed yet
        c.run(multiply=3) # Returns 3, fit will execute; state["value"] = 6
        # Some time later...
        c.run(multiply=4) # Returns 24
        ```

        Args:
            key (str):
                Keyword for the fit op.
            batch_size (int, optional):
                Number of values to wait for before
                calling the fit function. Defaults to 1.

        Returns:
            Callable: Decorated fit function.
        """

        def decorator(func: Callable) -> Any:
            if not validate_args(inspect.signature(func).parameters, "fit"):
                raise ValueError(
                    f"Fit method {func.__name__} should have 3 arguments: "
                    + "`state`, `values`, and `infer_results`."
                )

            func._input_key = key  # type: ignore
            func._batch_size = batch_size  # type: ignore
            func._op = "fit"  # type: ignore
            self._executor.add_route(
                func._input_key, func._op, func  # type: ignore
            )  # type: ignore
            return func

        return decorator

    def run(self, **kwargs: Any) -> Union[Any, Tuple[Any, FitEventGroup]]:
        """Runs the dataflow (infer and fit ops) for the keyword argument
        passed in. If the key is not found to have any ops, an error
        is raised. Only one keyword argument should be passed in.
        Fit ops are only executed when the batch size is reached.

        Example Usage:
        ```python
        from motion import Component

        c = Component("MyComponent")

        @c.init
        def setUp():
            return {"value": 0}

        @c.infer("add")
        def add(state, value):
            return state["value"] + value

        @c.fit("add", batch_size=2)
        def add(state, values):
            return {"value": state["value"] + sum(values)}

        @c.infer("multiply")
        def multiply(state, value):
            return state["value"] * value

        c.run(add=1, wait_for_fit=True) # (1)!
        # Ignore the previous line
        c.run(add=1) # Returns 1
        c.run(add=2, wait_for_fit=True) # Returns 2, result state["value"] = 3
        # Previous line called fit function and flushed fit queue
        result, fit_event = c.run(add=3, return_fit_event=True)
        print(result) # Prints 6
        fit_event.wait() # (2)!
        # Ignore the previous line
        c.run(multiply=2) # Returns 6 since state["value"] = 3
        c.run(multiply=3, wait_for_fit=True) # (3)!

        # 1. This will hang, since `batch_size=2` is not reached
        # 2. This will also hang, since the fit queue only has 1 task
        # 3. This throws an error, since there is no fit function for multiply
        ```


        Args:
            **kwargs:
                Keyword arguments for the infer and fit ops.
            return_fit_event (bool, optional):
                If True, returns an instance of `FitEventGroup` that is set
                when the fit function has finished executing. Defaults to False.
            wait_for_fit (bool, optional):
                If True, waits for the fit function to finish executing before
                returning. Defaults to False. Warning: if the fit queue is
                still waiting to get to batch_size elements, this will hang
                forever!

        Raises:
            ValueError: _description_

        Returns:
            Union[Any, Tuple[Any, FitEventGroup]]:
                Either the result of the infer function, or both
                the result of the infer function and the `FitEventGroup` if
                `return_fit_event` is True.
        """

        return_fit_event = kwargs.pop("return_fit_event", False)
        wait_for_fit = kwargs.pop("wait_for_fit", False)

        infer_result, fit_event = self._executor.run(**kwargs)

        if wait_for_fit:
            if not fit_event:
                raise ValueError(
                    "wait_for_fit=True, but there's no fit for this dataflow."
                )
            fit_event.wait()

        if return_fit_event:
            return infer_result, fit_event

        return infer_result
