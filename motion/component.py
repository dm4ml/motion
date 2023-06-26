import functools
import inspect
from typing import Any, Callable, Dict, List, Optional, Union, get_type_hints

from pydantic import BaseModel

from motion.instance import ComponentInstance
from motion.route import Route
from motion.utils import CustomDict, random_passphrase, validate_args


class Component:
    """Component class for creating Motion components.
    Here are some examples:

    === "Basic"
        ```python
        from motion import Component

        AdderComponent = Component("MyAdder")

        @AdderComponent.init_state
        def setUp():
            return {"value": 0}

        @AdderComponent.infer("add")
        def plus(state, value):
            return state["value"] + value

        @AdderComponent.fit("add")
        def add(state, values, infer_results):
            return {"value": state["value"] + sum(values)}

        if __name__ == "__main__":
            c = AdderComponent() # Create instance of AdderComponent
            c.run(add=1, flush_fit=True) # Will return 1, blocking until fit
            # is done. Resulting state is {"value": 1}
            c.run(add=2) # Will return 3, not waiting for fit operation.
            # Resulting state will eventually be {"value": 3}
        ```

    === "Multiple Dataflows"
        ```python
        from motion import Component

        Calculator = Component("MyCalculator")

        @Calculator.init_state
        def setUp():
            return {"value": 0}

        @Calculator.infer("add")
        def plus(state, value):
            return state["value"] + value

        @Calculator.fit("add")
        def increment(state, values, infer_results):
            return {"value": state["value"] + sum(values)}

        @Calculator.infer("subtract")
        def minus(state, value):
            return state["value"] - value

        @Calculator.fit("subtract")
        def decrement(state, values, infer_results):
            return {"value": state["value"] - sum(values)}

        if __name__ == "__main__":
            c = Calculator()
            c.run(add=1, flush_fit=True) # Will return 1, blocking until fit
            # is done. Resulting state is {"value": 1}
            c.run(subtract=1, flush_fit=True) # Will return 0, blocking
            # until fit is done. Resulting state is {"value": 0}
        ```

    === "Batch Size > 1"

        ```python
        from motion import Component
        import numpy as np

        MLMonitor = Component("Monitoring_ML_Component")

        @MLMonitor.init_state
        def setUp():
            return {"model": YOUR_MODEL_HERE, "history": []}

        @MLMonitor.infer("features")
        def predict(state, value):
            return state["model"].predict(value)

        @MLMonitor.fit("features", batch_size=10)
        def monitor(state, values, infer_results):
            new_X = np.array(values)
            new_y = np.array(infer_results)
            concatenated = np.concatenate((state["history"], new_y))
            if YOUR_ANOMALY_ALGORITHM(concatenated, history):
                # Fire an alert
                YOUR_ALERT_FUNCTION()
            return {"history": history + [concatenated]}

        if __name__ == "__main__":
            c = MLMonitor() # Create instance
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

        MyComponent = Component("MyComponentWithValidation")

        @MyComponent.infer("noop")
        def noop(state, value: MyModel):
            return value.value

        if __name__ == "__main__":
            c = MyComponent()
            c.run(noop=MyModel(value=1)) # Will return 1
            c.run(noop={"value": 1}) # Will return 1
            c.run(noop=MyModel(value="1")) # Will raise an Error
            c.run(noop=1) # Will raise an Error
        ```
    """

    def __init__(self, name: str, params: Dict[str, Any] = {}):
        """Creates a new Motion component.

        Args:
            name (str):
                Name of the component.
                params (Dict[str, Any], optional):
                    Parameters to be accessed by the component. Defaults to {}.
                    Usage: `C.params["param_name"]` if C is the Component you
                    have created.
        """
        self._name = name
        self._params = CustomDict(name, "params", "", params)

        # Set up routes
        self._infer_routes: Dict[str, Route] = {}
        self._fit_routes: Dict[str, List[Route]] = {}
        self._init_state_func: Optional[Callable] = None
        self._save_state_func: Optional[Callable] = None
        self._load_state_func: Optional[Callable] = None

    @property
    def name(self) -> str:
        """Name of the component.

        Example Usage:
        ```python
        from motion import Component

        MyComponent = Component("MyComponent")
        print(MyComponent.name) # Prints "MyComponent"
        ```

        Returns:
            str: Component name.
        """
        return self._name

    def add_route(self, key: str, op: str, udf: Callable) -> None:
        if op == "infer":
            if key in self._infer_routes.keys():
                raise ValueError(
                    f"Cannot have more than one infer route for key `{key}`."
                )

            self._infer_routes[key] = Route(key=key, op=op, udf=udf)
        elif op == "fit":
            if key not in self._fit_routes.keys():
                self._fit_routes[key] = []

            self._fit_routes[key].append(Route(key=key, op=op, udf=udf))

        else:
            raise ValueError(f"Invalid op `{op}`.")

    @property
    def params(self) -> Dict[str, Any]:
        """Parameters to use in component functions.

        Example Usage:
        ```python
        from motion import Component

        MyComponent = Component(
            "MyComponent",
            params={"param1": 1, "param2": 2}
        )

        @MyComponent.init_state
        def setUp():
            return {"value": 0}

        @MyComponent.infer("add")
        def plus(state, value):
            # Access params with MyComponent.params["param_name"]
            return state["value"] + value + MyComponent.params["param1"] +
            MyComponent.params["param2"]
        ```

        Returns:
            Dict[str, Any]: Parameters dictionary.
        """
        return self._params

    def init_state(self, func: Callable) -> Callable:
        """Decorator for the init_state function. This function
        is called once at the beginning of the component's lifecycle.
        The decorated function should return a dictionary that represents
        the initial state of the component.

        Usage:
        ```python
        from motion import Component

        MyComponent = Component("MyComponent")

        @MyComponent.init_state
        def setUp():
            return {"value": 0}
        ```

        Args:
            func (Callable): Function that initializes a state. Must return
                a dictionary.

        Returns:
            Callable: Decorated init_state function.
        """
        self._init_state_func = func
        return func

    def save_state(self, func: Callable) -> Callable:
        """Decorator for the save_state function. This function
        saves the state of the component to be accessible in
        future component instances of the same name.

        Usage:
        ```python
        from motion import Component

        MyComponent = Component("MyComponent")

        @c.save_state
        def save(state):
            # state might have other unpicklable keys, like a DB connection
            return {"fit_count": state["fit_count"]}
        ```

        Args:
            func (Callable): Function that returns a cloudpickleable object.

        Returns:
            Callable: Decorated save_state function.
        """
        self._save_state_func = func
        return func

    def load_state(self, func: Callable) -> Callable:
        """Decorator for the load_state function. This function
        loads the state of the component from the unpickled state.

        Usage:
        ```python
        from motion import Component

        MyComponent = Component("MyComponent")

        @c.load_state
        def load(state):
            conn = sqlite3.connect(":memory:")
            cursor = conn.cursor()
            return {"cursor": cursor, "fit_count": state["fit_count"]}
        ```

        Args:
            func (Callable): Function that consumes a cloudpickleable object.
                Should return a dictionary representing the state of the
                component instance.

        Returns:
            Callable: Decorated load_state function.
        """
        self._load_state_func = func
        return func

    def infer(self, keys: Union[str, List[str]]) -> Callable:
        """Decorator for any infer dataflow through the component. Takes
        in a string that represents the input keyword for the infer dataflow.

        2 arguments required for an infer operation:
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

        MyComponent = Component("MyComponent")

        @MyComponent.init_state
        def setUp():
            return {"value": 0}

        @MyComponent.infer("add")
        def add(state, value):
            return state["value"] + value

        @MyComponent.infer("multiply")
        def multiply(state, value):
            return state["value"] * value

        c = MyComponent()
        c.run(add=1, flush_fit=True) # Returns 1
        c.run(multiply=2) # Returns 2
        ```

        Args:
            keys (Union[str, List[str]]): String or list of strings that
                represent the input keyword(s) for the infer dataflow.

        Returns:
            Callable: Decorated infer function.
        """
        if isinstance(keys, str):
            keys = [keys]

        for key in keys:
            if "::" in key:
                raise ValueError(
                    f"Dataflow key {key} should not have a double colon (::)"
                )

        def decorator(func: Callable) -> Any:
            type_hint = get_type_hints(func).get("value", None)
            if not validate_args(inspect.signature(func).parameters, "infer"):
                raise ValueError(
                    f"Infer function {func.__name__} should have 2 arguments "
                    + "`state` and `value`"
                )

            @functools.wraps(func)
            def wrapper(state: CustomDict, value: Any) -> Any:
                if (
                    type_hint
                    and inspect.isclass(type_hint)
                    and issubclass(type_hint, BaseModel)
                    and not isinstance(value, type_hint)
                ):
                    try:
                        value = type_hint(**value)
                    except Exception:
                        raise ValueError(
                            f"value argument must be of type {type_hint.__name__}"
                        )

                return func(state, value)

            wrapper._op = "infer"  # type: ignore

            for key in keys:
                self.add_route(key, wrapper._op, wrapper)  # type: ignore

            return wrapper

        return decorator

    def fit(self, keys: Union[str, List[str]], batch_size: int = 1) -> Any:
        """Decorator for any fit dataflows through the component. Takes
        in a string that represents the input keyword for the fit op.
        Only executes the fit op (function) when the batch size is reached.

        3 arguments required for a fit operation:
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

        MyComponent = Component("MyComponent")

        @MyComponent.init_state
        def setUp():
            return {"value": 0}

        @MyComponent.fit("add")
        def add(state, values):
            return {"value": state["value"] + sum(values)}

        @MyComponent.infer("multiply")
        def multiply(state, value):
            return state["value"] * value

        @MyComponent.fit("multiply", batch_size=2) # Runs after 2 c.run calls
        def multiply(state, values, infer_results):
            product = 1
            for value in values:
                product *= value
            return state["value"] * product

        c = MyComponent()
        c.run(add=1, flush_fit=True) # Returns 1
        c.run(multiply=2) # Returns 2, fit not executed yet
        c.run(multiply=3) # Returns 3, fit will execute; state["value"] = 6
        # Some time later...
        c.run(multiply=4) # Returns 24
        ```

        Args:
            keys (Union[str, List[str]]): String or list of strings that
                represent the input keyword(s) for the fit dataflow.
            batch_size (int, optional):
                Number of values to wait for before
                calling the fit function. Defaults to 1.

        Returns:
            Callable: Decorated fit function.
        """
        frame = inspect.currentframe().f_back  # type: ignore
        fname = frame.f_code.co_name  # type: ignore
        if fname != "<module>":
            raise ValueError(
                f"Component {self.name} fit method must be defined in a module "
                + f"context. It's currently initialized from function {fname}."
            )
        if isinstance(keys, str):
            keys = [keys]

        def decorator(func: Callable) -> Any:
            if not validate_args(inspect.signature(func).parameters, "fit"):
                raise ValueError(
                    f"Fit method {func.__name__} should have 3 arguments: "
                    + "`state`, `values`, and `infer_results`."
                )

            # func._input_key = key  # type: ignore
            func._batch_size = batch_size  # type: ignore
            func._op = "fit"  # type: ignore

            for key in keys:
                self.add_route(key, func._op, func)  # type: ignore

            return func

        return decorator

    def __call__(
        self,
        name: str = "",
        init_state_params: Dict[str, Any] = {},
        logging_level: str = "WARNING",
    ) -> ComponentInstance:
        """Creates and returns a new instance of a Motion component.
        See `ComponentInstance` docs for more info.

        Usage:
        ```python
        from motion import Component

        MyComponent = Component("MyComponent")

        @MyComponent.init_state
        def setUp(starting_val):
            return {"value": starting_val}

        # Define infer and fit operations
        @MyComponent.infer("key1")
        def ...

        @MyComponent.fit("key1)
        def ...

        c_instance = MyComponent(init_state_params={"starting_val": 3})
        # Creates instance of MyComponent
        c_instance.run(..)
        ```

        Args:
            name (str, optional):
                Name of the component instance. Defaults to "".
            init_state_params (Dict[str, Any], optional):
                Parameters to pass into the init_state function. Defaults to {}.
            logging_level (str, optional):
                Logging level for the Motion logger. Uses the logging library.
                Defaults to "WARNING".
        Returns:
            ComponentInstance: Component instance to run dataflows with.
        """
        if not name:
            name = random_passphrase()

        if "__" in name:
            raise ValueError(
                f"Instance name {name} cannot contain '__'. Strip the component"
                + "name from your instance name."
            )

        instance_name = f"{self.name}__{name}"

        try:
            ci = ComponentInstance(
                component_name=self.name,
                instance_name=instance_name,
                init_state_func=self._init_state_func,
                init_state_params=init_state_params,
                save_state_func=self._save_state_func,
                load_state_func=self._load_state_func,
                infer_routes=self._infer_routes,
                fit_routes=self._fit_routes,
                logging_level=logging_level,
            )
        except RuntimeError:
            raise RuntimeError(
                "Error creating component instance. Make sure the entry point "
                + "of your program is protected, using `if __name__ == '__main__':`"
            )

        return ci

    def get_graph(self, x_offset_step: int = 600) -> Dict[str, Any]:
        """
        Gets the graph of infer and fit ops for this component.
        """

        graph: Dict[str, Dict[str, Any]] = {}

        for key, route in self._infer_routes.items():
            graph[key] = {
                "infer": {
                    "name": route.udf.__name__,
                    "udf": inspect.getsource(route.udf),
                },
            }

        for key, routes in self._fit_routes.items():
            if key not in graph:
                graph[key] = {}
            graph[key]["fit"] = []
            for route in routes:
                graph[key]["fit"].append(
                    {
                        "name": route.udf.__name__,
                        "udf": inspect.getsource(route.udf),
                        "batch_size": route.udf._batch_size,  # type: ignore
                    }
                )

        nodes = []
        edges = []
        node_id = 1
        max_x_offset = 0

        # Positions for layout
        x_offset = 200
        y_offset = 200
        key_y_positions = {}

        # Add state node
        state_node = {
            "id": str(node_id),
            "position": {"x": 0, "y": 0},
            "data": {"label": "state"},
            "type": "state",
        }
        node_id += 1

        for key, value in graph.items():
            # Assign y position for key nodes
            if key not in key_y_positions:
                key_y_positions[key] = y_offset
                y_offset += 100
            key_y_position = key_y_positions[key]

            # Add key node
            key_node = {
                "id": str(node_id),
                "position": {"x": 0, "y": key_y_position},
                "data": {"label": key},
                "type": "key",
            }
            nodes.append(key_node)
            node_id += 1

            # Assign x position for infer nodes
            infer_x_offset = x_offset

            if "infer" in value.keys():
                # Add infer node
                infer_node = {
                    "id": str(node_id),
                    "position": {"x": infer_x_offset, "y": key_y_position},
                    "data": {
                        "label": value["infer"]["name"],
                        "udf": value["infer"]["udf"],
                    },
                    "type": "infer",
                }
                nodes.append(infer_node)
                edges.append(
                    {
                        "id": "e{}-{}".format(key_node["id"], infer_node["id"]),
                        "source": key_node["id"],
                        "target": infer_node["id"],
                        "targetHandle": "left",
                    }
                )
                edges.append(
                    {
                        "id": "e{}-{}".format(state_node["id"], infer_node["id"]),
                        "source": state_node["id"],
                        "target": infer_node["id"],
                        "targetHandle": "top",
                    }
                )
                node_id += 1

            # Assign x position for fit nodes
            infer_x_offset += x_offset_step
            fit_x_offset = infer_x_offset

            if "fit" in value.keys():
                for fit in value["fit"]:
                    # Add fit node
                    fit_node = {
                        "id": str(node_id),
                        "position": {"x": fit_x_offset, "y": key_y_position},
                        "data": {
                            "label": fit["name"],
                            "udf": fit["udf"],
                            "batch_size": fit["batch_size"],
                        },
                        "type": "fit",
                    }
                    nodes.append(fit_node)

                    edges.append(
                        {
                            "id": "e{}-{}".format(fit_node["id"], state_node["id"]),
                            "target": state_node["id"],
                            "source": fit_node["id"],
                            "sourceHandle": "top",
                            "animated": True,  # type: ignore
                            "label": f"batch_size: {fit['batch_size']}",
                        }
                    )

                    if "infer" in value.keys():
                        edges.append(
                            {
                                "id": "e{}-{}".format(infer_node["id"], fit_node["id"]),
                                "source": infer_node["id"],
                                "sourceHandle": "right",
                                "target": fit_node["id"],
                                "targetHandle": "left",
                                "animated": True,  # type: ignore
                            }
                        )
                    else:
                        edges.append(
                            {
                                "id": "e{}-{}".format(key_node["id"], fit_node["id"]),
                                "source": key_node["id"],
                                "target": fit_node["id"],
                                "targetHandle": "left",
                            }
                        )

                    fit_x_offset += x_offset_step
                    node_id += 1

            if fit_x_offset > max_x_offset:
                max_x_offset = fit_x_offset

        # Update state x offset
        state_node["position"]["x"] = int(max_x_offset / 2)  # type: ignore
        nodes.append(state_node)

        return {"name": self.name, "nodes": nodes, "edges": edges}
