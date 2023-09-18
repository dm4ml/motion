"""
This file contains the props class, which is used to store
properties of a flow.
"""
from typing import Any, Optional

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

    # def __getattr__(self, key: str) -> object:
    #     return self.__getitem__(key)

    # def __setattr__(self, key: str, value: Any) -> None:
    #     self[key] = value

    # def __getstate__(self) -> dict:
    #     return dict(self)


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
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.component_name = component_name
        self._instance_id = instance_id
        super().__init__(*args, **kwargs)

    @property
    def instance_id(self) -> str:
        """
        Returns the instance id of the component.
        Useful if wanting to create other component instances
        within a serve or update operation.
        """
        return self._instance_id

    def __getitem__(self, key: str) -> object:
        try:
            return super().__getitem__(key)
        except KeyError:
            raise KeyError(
                f"Key `{key}` not found in state for "
                + f"instance {self.component_name}__{self._instance_id}."
            )


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
