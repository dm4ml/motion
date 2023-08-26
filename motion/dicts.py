"""
This file contains the props class, which is used to store
properties of a flow.
"""
from typing import Any, Iterator, List, Optional, Tuple

from motionstate import StateAccessor

import pandas as pd
import pyarrow as pa


class CustomDict(dict):
    def __init__(
        self,
        component_name: str,
        dict_type: str,
        instance_id: Optional[str] = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.component_name = component_name
        self.instance_id = instance_id
        self.dict_type = dict_type
        super().__init__(*args, **kwargs)

    def __getitem__(self, key: str) -> object:
        try:
            return super().__getitem__(key)
        except KeyError:
            raise KeyError(
                f"Key `{key}` not found in {self.dict_type} for "
                + f"instance {self.component_name}__{self.instance_id}."
            )


class Properties(dict):
    """Dictionary that stores properties of a flow.

    Example usage:

    ```python
    from motion import Component

    some_component = Component("SomeComponent")

    @some_component.init_state
    def setUp():
        return {"model": ...}

    @some_component.serve("image")
    def predict_image(state, props):
        # props["image_embedding"] is passed in at runtime
        return state["model"](props["image_embedding"])

    @some_component.update("image")
    def monitor_prediction(state, props):
        # props.serve_result is the result of the serve operation
        if props.serve_result > some_threshold:
            trigger_alert()

    if __name__ == "__main__":
        c = some_component()
        c.run("image", props={"image_embedding": ...})
    ```
    """

    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._serve_result = None
        super().__init__(*args, **kwargs)

    def __getitem__(self, key: str) -> object:
        try:
            return super().__getitem__(key)
        except KeyError:
            raise KeyError(f"Key `{key}` not found in props. ")

    @property
    def serve_result(self) -> Any:
        """Stores the result of the serve operation. Can be accessed
        in the update operation, not the serve operation.

        Returns:
            Any: Result of the serve operation.
        """
        return self._serve_result


STATE_ERROR_MSG = "Cannot edit state directly. Use component update operations instead."


class State(dict):
    """Dictionary that stores state for a component instance.
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

    def __setitem__(self, key: str, value: Any) -> None:
        # Disable this functionality
        raise NotImplementedError(STATE_ERROR_MSG)

    def flushUpdateDict(self, update_dict: dict) -> None:
        self._state_accessor.bulk_set(update_dict)

    def __delitem__(self, key: str) -> None:
        raise NotImplementedError(STATE_ERROR_MSG)

    def update(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(STATE_ERROR_MSG)

    def clear(self) -> None:
        raise NotImplementedError(STATE_ERROR_MSG)

    def pop(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(STATE_ERROR_MSG)

    def popitem(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(STATE_ERROR_MSG)

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
        return self._state_accessor.__iter__()


class MDataFrame(pd.DataFrame):
    """Wrapper around pandas DataFrame that allows for pyarrow-based
    serialization. This is to be used in a motion component's state.

    Simply use this class instead of pandas DataFrame. For example:
    ```python
    from motion import MDataFrame, Component

    C = Component("MyDFComponent")

    @C.init_state
    def setUp():
        df = MDataFrame({"value": [0, 1, 2]})
        return {"df": df}
    ```
    """

    def __getstate__(self) -> dict:
        # Serialize with pyarrow
        table = pa.Table.from_pandas(self)
        # Convert the PyArrow Table to a PyArrow Buffer
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()

        buffer = sink.getvalue()
        return {"table": buffer}

    def __setstate__(self, state: dict) -> None:
        # Convert the PyArrow Buffer to a PyArrow Table
        buf = state["table"]
        reader = pa.ipc.open_stream(buf)
        df = reader.read_pandas()
        self.__init__(df)  # type: ignore


class Params(dict):
    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

    def __getitem__(self, key: str) -> object:
        try:
            return super().__getitem__(key)
        except KeyError:
            raise KeyError(f"Key `{key}` not found in component params.")
