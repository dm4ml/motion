import atexit
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from motion.execute import Executor
from motion.route import Route
from motion.utils import DEFAULT_KEY_TTL, FitEventGroup, configureLogging, logger


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
        instance_name: str,
        init_state_func: Optional[Callable],
        init_state_params: Optional[Dict[str, Any]],
        save_state_func: Optional[Callable],
        load_state_func: Optional[Callable],
        infer_routes: Dict[str, Route],
        fit_routes: Dict[str, List[Route]],
        logging_level: str = "WARNING",
    ):
        """Creates a new instance of a Motion component.

        Args:
            name (str):
                Name of the component we are creating an instance of.
            instance_name (str):
                Name of the instance we are creating.
            logging_level (str, optional):
                Logging level for the Motion logger. Uses the logging library.
                Defaults to "WARNING".
        """
        self._component_name = component_name
        configureLogging(logging_level)
        # self._serverless = serverless
        # indicator = "serverless" if serverless else "local"
        logger.info(f"Creating local instance of {self._component_name}...")
        atexit.register(self.shutdown)

        # Create instance name
        self._instance_name = instance_name

        self.running = False
        self._executor = Executor(
            self._instance_name,
            init_state_func=init_state_func,
            init_state_params=init_state_params if init_state_params else {},
            save_state_func=save_state_func,
            load_state_func=load_state_func,
            infer_routes=infer_routes,
            fit_routes=fit_routes,
        )
        self.running = True

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
        if not self.running:
            return

        is_open = is_logger_open(logger)

        if is_open:
            logger.info(f"Shutting down {self._instance_name}...")

        self._executor.shutdown(is_open=is_open)

        self.running = False

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
        return self._executor._loadState()[key]

    def run(
        self,
        *,
        cache_ttl: int = DEFAULT_KEY_TTL,
        force_refresh: bool = False,
        force_fit: bool = False,
        **kwargs: Any,
    ) -> Union[Any, Tuple[Any, FitEventGroup]]:
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
        def add(state, values, infer_results):
            return {"value": state["value"] + sum(values)}

        @C.infer("multiply")
        def multiply(state, value):
            return state["value"] * value

        c = C() # Create instance of C
        c.run(add=1, force_fit=True) # (1)!
        c.run(add=1) # Returns 1
        c.run(add=2, force_fit=True) # Returns 2, result state["value"] = 4
        # Previous line called fit function and flushed fit queue
        c.run(add=3) # No fit op runs since batch size = 1
        c.run(multiply=2) # Returns 8 since state["value"] = 4
        c.run(multiply=3, force_fit=True) # (2)!

        # 1. This forces the fit op to run even though the batch size
        #   isn't reached, and waits for the fit op to finish running
        # 2. This doesn't force or wait for any fit ops, since there are
        #   no fit ops defined for `multiply`
        ```


        Args:
            cache_ttl (int, optional):
                How long the inference result should live in a cache (in
                seconds). Defaults to 1 day (60 * 60 * 24). The expiration
                time is extended if there are subsequent infer calls
                for the same value.
            force_refresh (bool, optional): Read the latest value of the
                state before running an inference call, otherwise a stale
                version of the state or a cached result may be used.
                If you do not want to read from the cache, set force_refresh
                = True. Defaults to False.
            force_fit (bool, optional):
                If True, waits for the fit op to finish executing before
                returning. If the fit queue hasn't reached batch_size
                yet, the fit op runs anyways. Force refreshes the
                state after the fit op completes. Defaults to False.
            **kwargs:
                Keyword arguments for the infer and fit ops. You can only
                pass in one pair.

        Raises:
            ValueError: If more than one dataflow key-value pair is passed.

        Returns:
            Any: Result of the inference call. Might take a long time
            to run if `force_fit = True` and the fit operation is
            computationally expensive.
        """
        if len(kwargs) != 1:
            raise ValueError("Only one key-value pair is allowed in kwargs.")

        key, value = next(iter(kwargs.items()))

        infer_result = self._executor.run(
            key=key,
            value=value,
            cache_ttl=cache_ttl,
            force_refresh=force_refresh,
            force_fit=force_fit,
        )

        return infer_result
