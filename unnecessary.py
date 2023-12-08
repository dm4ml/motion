from typing import Any, Awaitable, Dict, Optional

import httpx
import requests


class ComponentInstanceClient:
    def __init__(
        self,
        component_name: str,
        instance_id: str,
        uri: str,
        access_token: str,
        **kwargs: Any,
    ):
        """Creates a new instance of a Motion component.

        Args:
            component_name (str):
                Name of the component we are creating an instance of.
            instance_id (str):
                ID of the instance we are creating.
        """
        self._component_name = component_name

        # Create instance name
        self._instance_name = f"{self._component_name}__{instance_id}"

        self.uri = uri
        self.access_token = access_token

        self.kwargs = kwargs

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

    def write_state(self, state_update: Dict[str, Any], latest: bool = False) -> None:
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
            latest (bool, optional): Whether or not to apply the update
                to the latest version of the state.
                If true, Motion will redownload the latest version
                of the state and apply the update to that version. You
                only need to set this to true if you are updating an
                instance you connected to a while ago and might be
                outdated. Defaults to False.
        """
        # Ask server to update state
        response = requests.post(
            f"{self.uri}/update_state",
            json={
                "instance_id": self.instance_id,
                "state_update": state_update,
                "kwargs": {"latest": latest},
            },
            headers={"Authorization": f"Bearer {self.access_token}"},
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to update state for instance {self.instance_id}: {response.text}"
            )

    def read_state(self, key: str, default_value: Optional[Any] = None) -> Any:
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
            default_value (Optional[Any], optional): Default value to return
                if the key is not found. Defaults to None.

        Returns:
            Any: Current value for the key, or default_value if the key
            is not found.
        """
        # Ask server to read state
        response = requests.get(
            f"{self.uri}/read_state",
            params={"instance_id": self.instance_id, "key": key},
            headers={"Authorization": f"Bearer {self.access_token}"},
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to read state for instance {self.instance_id}: {response.text}"
            )

        # Get response
        result = response.json()["value"]
        if not result:
            return default_value

        return result

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
                If flush_update is called and the component instance update
                processes are disabled.

        Returns:
            Any: Result of the serve call. Might take a long time
            to run if `flush_update = True` and the update operation is
            computationally expensive.
        """

        # Ask server to run dataflow
        response = requests.post(
            f"{self.uri}/{self.component_name}",
            json={
                "component_name": self.component_name,
                "instance_id": self.instance_id,
                "dataflow_key": dataflow_key,
                "is_async": False,
                "props": props,
                "kwargs": {
                    "ignore_cache": ignore_cache,
                    "force_refresh": force_refresh,
                    "flush_update": flush_update,
                },
            },
            headers={"Authorization": f"Bearer {self.access_token}"},
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to run dataflow for instance {self.instance_id}: {response.text}"
            )

        # Get response
        result = response.json()["value"]
        return result

    async def arun(
        self,
        # *,
        dataflow_key: str,
        props: Dict[str, Any] = {},
        ignore_cache: bool = False,
        force_refresh: bool = False,
        flush_update: bool = False,
    ) -> Awaitable[Any]:
        """Async version of run. Runs the dataflow (serve and update ops) for
        the specified key. You should use arun if either the serve or update op
        is an async function.

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
            If flush_update is called and the component instance update
                processes are disabled.

        Returns:
            Awaitable[Any]: Awaitable Result of the serve call.
        """

        # Ask server to run dataflow asynchronously
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.uri}/{self.component_name}",
                json={
                    "component_name": self.component_name,
                    "instance_id": self.instance_id,
                    "dataflow_key": dataflow_key,
                    "is_async": True,
                    "props": props,
                    "kwargs": {
                        "ignore_cache": ignore_cache,
                        "force_refresh": force_refresh,
                        "flush_update": flush_update,
                    },
                },
                headers={"Authorization": f"Bearer {self.access_token}"},
            )
            response.raise_for_status()
            return response.json()["value"]
