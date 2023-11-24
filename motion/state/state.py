"""
This file contains the state class.
"""
from typing import Any, Iterator, List, Optional, Tuple

from motionstate import StateAccessor

STATE_ERROR_MSG = "Cannot edit state directly. Use component update operations instead."


class State:
    """Python class that stores state for a component instance.
    The instance id is stored in the `instance_id` attribute.

    Example usage:

    ```python
    from motion import Component

    some_component = Component("SomeComponent")

    @some_component.init_state
    def setUp():
        return {"model": ...}

    @some_component.serve("retrieve")
    def retrieve_nn(state, props):
        # model can be accessed via state["model"]
        prediction = state["model"](props["image_embedding"])
        # match the prediction to some other data to do a retrieval
        nn_component_instance = SomeOtherMotionComponent(state.instance_id)
        return nn_component_instance.run("retrieve", props={"prediction": prediction})

    if __name__ == "__main__":
        c = some_component()
        nearest_neighbors = c.run("retrieve", props={"image_embedding": ...})
    ```
    """

    def __init__(
        self,
        component_name: str,
        instance_id: str,
        redis_host: str,
        redis_port: int,
        redis_db: int = 0,
        redis_password: Optional[str] = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.component_name = component_name
        self._instance_id = instance_id
        self._state_accessor = StateAccessor(
            component_name,
            instance_id,
            1000 * 60 * 2,  # 2 minutes lock duration TODO: make this configurable
            redis_host,
            redis_port,
            redis_db,
            redis_password,
        )
        super().__init__(*args, **kwargs)

    def get_version(self) -> int:
        return self._state_accessor.version

    @property
    def instance_id(self) -> str:
        """
        Returns the instance id of the component.
        Useful if wanting to create other component instances
        within a serve or update operation.
        """
        return self._instance_id

    def clear_cache(self) -> None:
        # Clear the cache
        self._state_accessor.clear_cache()

    def __getitem__(self, key: str) -> object:
        try:
            # Get from state accessor
            return self._state_accessor.get(key)
        except KeyError:
            raise KeyError(
                f"Key `{key}` not found in state for "
                + f"instance {self.component_name}__{self._instance_id}."
            )

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def __setitem__(self, key: str, value: Any) -> None:
        # Disable this functionality
        raise RuntimeError(STATE_ERROR_MSG)

    def flushUpdateDict(self, update_dict: dict, from_migration: bool = False) -> None:
        self._state_accessor.bulk_set(update_dict, from_migration)

    def __delitem__(self, key: str) -> None:
        raise RuntimeError(STATE_ERROR_MSG)

    def update(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError(STATE_ERROR_MSG)

    def clear(self) -> None:
        raise RuntimeError(STATE_ERROR_MSG)

    def pop(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError(STATE_ERROR_MSG)

    def popitem(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError(STATE_ERROR_MSG)

    def keys(self) -> List[str]:
        return self._state_accessor.keys()

    def values(self) -> List[Any]:
        """Values in the state dictionary.

        Note: This fetches all the values from the state. We
        do not recommend using this method as it can be slow.
        Consider accessing values directly via `state[key]`.

        Returns:
            List[Any]: List of values in the state.
        """

        return self._state_accessor.values()

    def items(self) -> List[Tuple[str, Any]]:
        """Items in the state dictionary.

        Note: This fetches all the key-value pairs from the state.
        We do not recommend using this method as it can be slow.
        If you need to iterate over the state, conditionally accessing
        values, we recommend using the `keys` method instead and then
        calling `state[key]` to access the value.

        Returns:
            List[Tuple[str, Any]]: List of key-value pairs in the state.
        """
        return self._state_accessor.items()

    def __iter__(self) -> Iterator[str]:
        return iter(self._state_accessor.keys())
