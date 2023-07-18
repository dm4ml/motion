"""
This file contains the props class, which is used to store
properties of a flow.
"""
from enum import Enum
from typing import Any, Callable, Dict, Optional

import cloudpickle
import msgpack
import redis


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

    def pack(self) -> Any:
        data = {
            "serve_result": self._serve_result,
            "properties": dict(self),
        }
        return msgpack.packb(data)

    @staticmethod
    def unpack(data: Optional[Any]) -> "Properties":
        unpacked_data = msgpack.unpackb(data)
        serve_result = unpacked_data["serve_result"]
        properties = unpacked_data["properties"]

        props = Properties(properties)
        props._serve_result = serve_result

        return props

    # def __getattr__(self, key: str) -> object:
    #     return self.__getitem__(key)

    # def __setattr__(self, key: str, value: Any) -> None:
    #     self[key] = value

    # def __getstate__(self) -> dict:
    #     return dict(self)


class StateContext(Enum):
    USER = 1
    EXECUTOR = 2
    INSPECTOR = 3
    MIGRATOR = 4


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
        redis_con: redis.Redis,
        context: StateContext,
        load_state_func: Optional[Callable] = None,
        save_state_func: Optional[Callable] = None,
        init_dict: Optional[dict] = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._component_name = component_name
        self._instance_id = instance_id
        self._redis_con = redis_con

        self._context = context
        self._load_state_func = load_state_func
        self._save_state_func = save_state_func

        # If init_dict is passed in, then it is the first time this component
        # instance is being run
        if init_dict:
            if self._load_state_func:
                init_dict = self._load_state_func(init_dict)
            super().__init__(*args, **kwargs)
            self.customUpdate(init_dict)

        else:
            # If the context is a migrator or inspector,
            # then we should load all key-value pairs
            # Or if there is a load_state_func,
            # then we should use that to load the state
            if (
                self._context == StateContext.MIGRATOR
                or self._context == StateContext.INSPECTOR
                or self._load_state_func
            ):
                # Load all keys from redis
                loaded_dict = self._load_all_keys()
                if self._load_state_func:
                    loaded_dict = self._load_state_func(loaded_dict)
                super().__init__(**loaded_dict)

            # Otherwise, load passed-in key-value pairs
            else:
                super().__init__(*args, **kwargs)

    def _load_all_keys(self) -> dict:
        """Loads all keys from redis into the state dictionary."""
        result = {}
        prefix = f"MOTION_STATE:{self._component_name}__{self._instance_id}/"
        # Scan through all keys in redis
        for key in self._redis_con.scan_iter(f"{prefix}*"):
            # Remove the prefix
            key = key.decode("utf-8").replace(prefix, "")
            # Load the value
            value = self._redis_con.get(f"{prefix}{key}")
            result[key] = load_state_value(value)  # type: ignore
        return result

    @property
    def instance_id(self) -> str:
        """
        Returns the instance id of the component.
        Useful if wanting to create other component instances
        within a serve or update operation.
        """
        return self._instance_id

    def __getitem__(self, key: str) -> object:
        if not isinstance(key, str):
            raise TypeError(f"State key {key} must be a string, not {type(key)}.")

        try:
            return super().__getitem__(key)
        except KeyError:
            # Get the key from redis
            value = self._redis_con.get(
                f"MOTION_STATE:{self._component_name}__{self._instance_id}/{key}"
            )
            if not value:
                raise KeyError(
                    f"Key `{key}` not found in state for "
                    + f"instance {self._component_name}__{self._instance_id}."
                )

            return load_state_value(value)

    def customUpdate(self, updates: Optional[Dict[Any, Any]]) -> None:
        if updates is None:
            return

        # Call original update and increment the version number
        super().update(updates)

        # If there is a save_state_func, then we should use that to save the state
        pipeline = self._redis_con.pipeline()

        if self._save_state_func:
            state_to_save = self._save_state_func(self)
            # Save each key-value pair to redis
            # Set the key-value-pair in redis if there is no save_state_func
            for key, value in state_to_save.items():
                pipeline.set(
                    f"MOTION_STATE:{self._component_name}__{self._instance_id}/{key}",
                    pack_state_value(value),
                )
        else:
            # Write each key-value pair in the updates to redis
            for key, value in updates.items():
                pipeline.set(
                    f"MOTION_STATE:{self._component_name}__{self._instance_id}/{key}",
                    pack_state_value(value),
                )

        pipeline.execute()

        self._redis_con.incr(
            f"MOTION_VERSION:{self._component_name}__{self._instance_id}"
        )

    def __setitem__(self, key: str, value: Any) -> None:
        if not isinstance(key, str):
            raise TypeError(f"State key {key} must be a string, not {type(key)}.")

        # Check context
        if self._context == StateContext.USER:
            raise ValueError(
                "State can only be modified by returning a dictionary of "
                + "updates in init and update operations."
            )

        # Set the key-value-pair in the state dictionary
        super().__setitem__(key, value)


def pack_state_value(value: object) -> Any:
    # First try to pack the value as a msgpack
    try:
        return msgpack.packb(value)
    except Exception:
        # Try to pack the value as a cloudpickle
        return cloudpickle.dumps(value)


def load_state_value(loaded_value: bytes) -> object:
    # First try to load the value as a msgpack
    try:
        return msgpack.unpackb(loaded_value)
    except Exception:
        # Try to load the value as a cloudpickle
        try:
            return cloudpickle.loads(loaded_value)
        except Exception:
            raise ValueError(
                "Unable to load state value. "
                + "Value is not a msgpack or cloudpickle."
            )


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
