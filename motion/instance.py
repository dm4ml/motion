import atexit
import logging
import os
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import cloudpickle
import redis

from motion.execute import Executor
from motion.route import Route
from motion.utils import CustomDict, FitEventGroup, configureLogging, logger


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
        cleanup: bool = False,
        logging_level: str = "WARNING",
        serverless: bool = False,
        redis_con: Optional[redis.Redis] = None,
    ):
        """Creates a new instance of a Motion component.

        Args:
            name (str):
                Name of the component we are creating an instance of.
            instance_name (str):
                Name of the instance we are creating.
            cleanup (bool, optional):
                Whether to process the remainder of fit events after the user
                shuts down the program. Defaults to False.
            logging_level (str, optional):
                Logging level for the Motion logger. Uses the logging library.
                Defaults to "WARNING".
            serverless (bool, optional): Whether to run the component in
                serverless mode, using Modal. Defaults to False.
        """
        self._component_name = component_name
        configureLogging(logging_level)
        self._serverless = serverless
        indicator = "serverless" if serverless else "local"
        logger.info(f"Creating {indicator} instance of {self._component_name}...")

        # Set up redis connection
        if not redis_con:
            atexit.register(self.shutdown)
        self._redis_con = self._connectToRedis() if redis_con is None else redis_con
        self._running = True

        # Create instance name
        self._instance_name = instance_name
        self._init_state_func = init_state_func
        self._load_state_func = load_state_func
        self._save_state_func = save_state_func

        # Set up state
        empty_state = CustomDict(self._instance_name, "state", {})
        initial_state = self.loadState(empty_state, **init_state_params)

        self._executor = Executor(
            self._instance_name,
            initial_state,
            infer_routes,
            fit_routes,
            cleanup=cleanup,
            redis_con=self._redis_con,
        )

    def _connectToRedis(self) -> redis.Redis:
        host = os.getenv("MOTION_REDIS_HOST", "localhost")
        port = os.getenv("MOTION_REDIS_PORT", 6379)
        password = os.getenv("MOTION_REDIS_PASSWORD", None)

        r = redis.Redis(
            host=host,
            port=port,
            password=password,
        )
        return r

    def loadState(self, state: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        # Get state from redis
        loaded_state = self._redis_con.get(f"MOTION:{self._instance_name}")

        if not loaded_state:
            # Set up initial state
            return self.setUp(**kwargs)

        # Unpickle state
        loaded_state = cloudpickle.loads(loaded_state)

        if self._load_state_func is not None:
            state.update(self._load_state_func(loaded_state))
        else:
            state.update(loaded_state)

        return state

    def saveState(self, state_to_save: Dict[str, Any]) -> None:
        # Save state to redis
        if self._save_state_func is not None:
            state_to_save = self._save_state_func(state_to_save)

        state_to_save = cloudpickle.dumps(state_to_save)

        self._redis_con.set(f"MOTION:{self._instance_name}", state_to_save)

    def setUp(self, **kwargs: Any) -> Dict[str, Any]:
        # Set up initial state
        if self._init_state_func is not None:
            initial_state = self._init_state_func(**kwargs)
            if not isinstance(initial_state, dict):
                raise TypeError(f"{self._instance_name} init should return a dict.")
            return initial_state

        return {}

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
        if not self._running:
            return

        is_open = is_logger_open(logger)

        if is_open:
            logger.info(f"Shutting down {self._instance_name}...")

        final_state = self._executor.shutdown(is_open=is_open)

        # Save state
        if is_open:
            logger.info(f"Saving state from {self._instance_name}...")

        self.saveState(final_state)

        if is_open:
            logger.info(f"Finished shutting down {self._instance_name}.")

        self._running = False

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
