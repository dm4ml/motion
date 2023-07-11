import atexit
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional

from motion.execute import Executor
from motion.route import Route
from motion.utils import DEFAULT_KEY_TTL, configureLogging, logger


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
        instance_id: str,
        init_state_func: Optional[Callable],
        init_state_params: Optional[Dict[str, Any]],
        save_state_func: Optional[Callable],
        load_state_func: Optional[Callable],
        serve_routes: Dict[str, Route],
        update_routes: Dict[str, List[Route]],
        logging_level: str = "WARNING",
        disabled: bool = False,
        cache_ttl: int = DEFAULT_KEY_TTL,
    ):
        """Creates a new instance of a Motion component.

        Args:
            component_name (str):
                Name of the component we are creating an instance of.
            instance_id (str):
                ID of the instance we are creating.
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
        self._instance_name = f"{self._component_name}__{instance_id}"
        self._cache_ttl = cache_ttl

        self.running = False
        self.disabled = disabled
        self._executor = Executor(
            self._instance_name,
            cache_ttl=self._cache_ttl,
            init_state_func=init_state_func,
            init_state_params=init_state_params if init_state_params else {},
            save_state_func=save_state_func,
            load_state_func=load_state_func,
            serve_routes=serve_routes,
            update_routes=update_routes,
            disabled=self.disabled,
        )
        self.running = True

    @property
    def instance_name(self) -> str:
        """Component name with a random phrase to represent
        the name of this instance.
        In the form of componentname__randomphrase.
        """
        return self._instance_name

    @property
    def instance_id(self) -> str:
        """Latter part of the instance name, which is a random phrase
        or a user-defined ID."""
        return self._instance_name.split("__")[-1]

    def shutdown(self) -> None:
        """Shuts down a Motion component instance, saving state.
        Automatically called when the instance is garbage collected.

        Usage:
        ```python
        from motion import Component

        C = Component("MyComponent")

        @C.init_state
        def setUp():
            return {"value": 0}

        # Define serve and update operations

        if __name__ == "__main__":
            c_instance = C()
            c_instance.run(...)
            c_instance.run(...)
            c_instance.shutdown()
        ```
        """
        if self.disabled:
            return

        if not self.running:
            return

        is_open = is_logger_open(logger)

        if is_open:
            logger.info(f"Shutting down {self._instance_name}...")

        self._executor.shutdown(is_open=is_open)

        self.running = False

    def get_version(self) -> int:
        """
        Gets the state version (might be outdated) currently being
        used for serve ops.

        Usage:
        ```python
        from motion import Component

        C = Component("MyComponent")

        @C.init_state
        def setUp():
            return {"value": 0}

        # Define serve and update operations

        if __name__ == "__main__":
            c_instance = C()
            c_instance.get_version() # Returns 1 (first version)
        ```
        """
        return self._executor.version  # type: ignore

    def write_state(self, state_update: Dict[str, Any]) -> None:
        """Writes the state update to the component instance's state.
        If a update op is currently running, the state update will be
        applied after the update op is finished. Warning: this could
        take a while if your update ops take a long time!

        Usage:
        ```python
        from motion import Component

        C = Component("MyComponent")

        @C.init_state
        def setUp():
            return {"value": 0}

        # Define serve and update operations
        ...

        if __name__ == "__main__":
            c_instance = C()
            c_instance.read_state("value") # Returns 0
            c_instance.write_state({"value": 1, "value2": 2})
            c_instance.read_state("value") # Returns 1
            c_instance.read_state("value2") # Returns 2
        ```

        Args:
            state_update (Dict[str, Any]): Dictionary of key-value pairs
                to update the state with.
        """
        self._executor._updateState(state_update)

    def read_state(self, key: str) -> Any:
        """Gets the current value for the key in the component instance's state.

        Usage:
        ```python
        from motion import Component

        C = Component("MyComponent")

        @C.init_state
        def setUp():
            return {"value": 0}

        # Define serve and update operations
        ...

        if __name__ == "__main__":
            c_instance = C()
            c_instance.read_state("value") # Returns 0
            c_instance.run(...)
            c_instance.read_state("value") # This will return the current value
            # of "value" in the state
        ```

        Args:
            key (str): Key in the state to get the value for.

        Returns:
            Any: Current value for the key.
        """
        return self._executor._loadState()[key]

    def flush_update(self, dataflow_key: str) -> None:
        """Flushes the update queue corresponding to the dataflow
        key, if it exists, and updates the instance state.
        Warning: this is a blocking operation and could take
        a while if your update op takes a long time!

        Example Usage:
        ```python
        from motion import Component

        C = Component("MyComponent")

        @C.init_state
        def setUp():
            return {"value": 0}

        @C.serve("add")
        def add(state, value):
            return state["value"] + value

        @C.update("add")
        def add(state, value):
            return {"value": state["value"] + value}

        @C.serve("multiply")
        def multiply(state, value):
            return state["value"] * value

        if __name__ == "__main__":
            c = C() # Create instance of C
            c.run("add", props={"value": 1})
            c.flush_update("add") # (1)!
            c.run("add", props={"value": 2}) # This will use the updated state

        # 1. Waits for the update op to finish, then updates the state
        ```

        Args:
            dataflow_key (str): Key of the dataflow.

        Raises:
            RuntimeError: If the component instance was initialized as disabled.
        """
        if self.disabled:
            raise RuntimeError("Cannot run a disabled component instance.")

        self._executor.flush_update(dataflow_key)

    def run(
        self,
        # *,
        dataflow_key: str,
        props: Dict[str, Any] = {},
        ignore_cache: bool = False,
        force_refresh: bool = False,
        flush_update: bool = False,
    ) -> Any:
        """Runs the dataflow (serve and update ops) for the keyword argument
        passed in. If the key is not found to have any ops, an error
        is raised. Only one dataflow key should be passed in.

        Example Usage:
        ```python
        from motion import Component

        C = Component("MyComponent")

        @C.init_state
        def setUp():
            return {"value": 0}

        @C.serve("add")
        def add(state, value):
            return state["value"] + value

        @C.update("add")
        def add(state, value):
            return {"value": state["value"] + value}

        if __name__ == "__main__":
            c = C() # Create instance of C
            c.run("add", props={"value": 1}, flush_update=True) # (1)!
            c.run("add", props={"value": 1}) # Returns 1
            c.run("add", props={"value": 2}, flush_update=True) # (2)!

            c.run("add", props={"value": 3})
            time.sleep(3) # Wait for the previous update op to finish

            c.run("add", props={"value": 3}, force_refresh=True) # (3)!

        # 1. Waits for the update op to finish, then updates the state
        # 2. Returns 2, result state["value"] = 4
        # 3. Force refreshes the state before running the dataflow, and
        #    reruns the serve op even though the result might be cached.
        ```


        Args:
            dataflow_key (str): Key of the dataflow to run.
            props (Dict[str, Any]): Keyword arguments to pass into the
                dataflow ops, in addition to the state.
            ignore_cache (bool, optional):
                If True, ignores the cache and runs the serve op. Does not
                force refresh the state. Defaults to False.
            force_refresh (bool, optional): Read the latest value of the
                state before running an serve call, otherwise a stale
                version of the state or a cached result may be used.
                Defaults to False.
            flush_update (bool, optional):
                If True, waits for the update op to finish executing before
                returning. If the update queue hasn't reached batch_size
                yet, the update op runs anyways. Force refreshes the
                state after the update op completes. Defaults to False.

         Raises:
            ValueError: If more than one dataflow key-value pair is passed.
            RuntimeError:
                If the component instance was initialized as disabled.

        Returns:
            Any: Result of the serve call. Might take a long time
            to run if `flush_update = True` and the update operation is
            computationally expensive.
        """
        if self.disabled:
            raise RuntimeError("Cannot run a disabled component instance.")

        serve_result = self._executor.run(
            key=dataflow_key,
            props=props,
            ignore_cache=ignore_cache,
            force_refresh=force_refresh,
            flush_update=flush_update,
        )

        return serve_result

    async def arun(
        self,
        # *,
        dataflow_key: str,
        props: Dict[str, Any] = {},
        ignore_cache: bool = False,
        force_refresh: bool = False,
        flush_update: bool = False,
    ) -> Awaitable[Any]:
        """Async version of run. Runs the dataflow (serve and update ops) for the
        specified key.

        Example Usage:
        ```python
        from motion import Component
        import asyncio

        C = Component("MyComponent")

        @C.serve("sleep")
        async def sleep(state, value):
            await asyncio.sleep(value)
            return "Slept!"

        async def main():
            c = C()
            await c.arun("sleep", props={"value": 1})

        if __name__ == "__main__":
            asyncio.run(main())
        ```

        Args:
            dataflow_key (str): Key of the dataflow to run.
            props (Dict[str, Any]): Keyword arguments to pass into the
                dataflow ops, in addition to the state.
            ignore_cache (bool, optional):
                If True, ignores the cache and runs the serve op. Does not
                force refresh the state. Defaults to False.
            force_refresh (bool, optional): Read the latest value of the
                state before running an serve call, otherwise a stale
                version of the state or a cached result may be used.
                Defaults to False.
            flush_update (bool, optional):
                If True, waits for the update op to finish executing before
                returning. If the update queue hasn't reached batch_size
                yet, the update op runs anyways. Force refreshes the
                state after the update op completes. Defaults to False.

        Raises:
            ValueError: If more than one dataflow key-value pair is passed.
            RuntimeError:
                If the component instance was initialized as disabled.

        Returns:
            Awaitable[Any]: Awaitable Result of the serve call.
        """
        if self.disabled:
            raise RuntimeError("Cannot run a disabled component instance.")

        serve_result = await self._executor.arun(
            key=dataflow_key,
            props=props,
            # value=value,
            ignore_cache=ignore_cache,
            force_refresh=force_refresh,
            flush_update=flush_update,
        )  # type: ignore

        return serve_result  # type: ignore
