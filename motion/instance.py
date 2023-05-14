import atexit
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from motion.execute import Executor
from motion.route import Route
from motion.utils import FitEventGroup, configureLogging, logger, random_passphrase


def is_logger_open(logger: logging.Logger) -> bool:
    for handler in logger.handlers:
        if (
            hasattr(handler, "stream")
            and handler.stream is not None
            and not handler.stream.closed
        ):
            return True
    return False


class ComponentInstance:
    def __init__(
        self,
        component_name: str,
        init_state_func: Optional[Callable],
        infer_routes: Dict[str, Route],
        fit_routes: Dict[str, List[Route]],
        cleanup: bool = False,
        logging_level: str = "WARNING",
    ):
        """Creates a new instance of a Motion component.

        Args:
            name (str):
                Name of the component we are creating an instance of.
            cleanup (bool, optional):
                Whether to process the remainder of fit events after the user
                shuts down the program. Defaults to False.
            logging_level (str, optional):
                Logging level for the Motion logger. Uses the logging library.
                Defaults to "WARNING".
        """
        self._component_name = component_name
        configureLogging(logging_level)

        # Create instance name
        self._instance_name = f"{self._component_name}__{random_passphrase()}"

        self._executor = Executor(
            self._instance_name,
            init_state_func,
            infer_routes,
            fit_routes,
            cleanup=cleanup,
        )

        atexit.register(self.shutdown)

    @property
    def instance_name(self) -> str:
        """Component name with a random phrase to represent
        the name of this instance."""
        return self._instance_name

    def shutdown(self) -> None:
        """Shuts down a Motion component instance, saving state.
        The state saving functionality is not implemented yet, but the
        graceful shutdown is.

        Usage:
        ```python
        from motion import Component

        C = Component("MyComponent")

        @C.init_state
        def setUp():
            return {"value": 0}

        # Define infer and fit operations

        c_instance = C()
        c_instance.run(...)
        c_instance.run(...)
        c_instance.shutdown()
        ```
        """
        is_open = is_logger_open(logger)

        if is_open:
            logger.info(f"Shutting down {self._instance_name}...")

        self._executor.shutdown(is_open=is_open)

        if is_open:
            logger.info(f"Saving state from {self._instance_name}...")
            # TODO: Save state
            logger.warning("State saving not implemented yet.")
            logger.info(f"Finished shutting down {self._instance_name}.")

    def read_state(self, key: str) -> Any:
        """Gets the current value for the key in the component's state.

        Usage:
        ```python
        from motion import Component

        C = Component("MyComponent")

        @C.init_state
        def setUp():
            return {"value": 0}

        # Define infer and fit operations

        c_instance = C()
        c_instance.read_state("value") # Returns 0
        c_instance.run(...)
        c_instance.read_state("value") # This will return the current value of
        # "value" in the state
        ```

        Args:
            key (str): Key in the state to get the value for.

        Returns:
            Any: Current value for the key.
        """
        return self._executor.state[key]

    def run(self, **kwargs: Any) -> Union[Any, Tuple[Any, FitEventGroup]]:
        """Runs the dataflow (infer and fit ops) for the keyword argument
        passed in. If the key is not found to have any ops, an error
        is raised. Only one keyword argument should be passed in.
        Fit ops are only executed when the batch size is reached.

        Example Usage:
        ```python
        from motion import Component

        C = Component("MyComponent")

        @C.init_state
        def setUp():
            return {"value": 0}

        @C.infer("add")
        def add(state, value):
            return state["value"] + value

        @C.fit("add", batch_size=2)
        def add(state, values):
            return {"value": state["value"] + sum(values)}

        @C.infer("multiply")
        def multiply(state, value):
            return state["value"] * value

        c = C() # Create instance of C
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
