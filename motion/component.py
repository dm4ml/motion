import inspect
from typing import Any, Callable, Dict, List, Literal, Optional, Union

from motion.dicts import Params
from motion.instance import ComponentInstance
from motion.route import Route
from motion.utils import DEFAULT_KEY_TTL, random_passphrase, validate_args


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

        @AdderComponent.serve("add")
        def plus(state, props):
            return state["value"] + props["value"]

        @AdderComponent.update("add")
        def add(state, props):
            return {"value": props.serve_result}

        if __name__ == "__main__":
            c = AdderComponent() # Create instance of AdderComponent
            c.run("add", props={"value": 1}, flush_update=True) # Blocks
            # until update is done. Resulting state is {"value": 1}
            c.run("add", props={"value": 2}) # Will return 3, not waiting
            # for update operation.
            # Resulting state will eventually be {"value": 3}
        ```

    === "Multiple Dataflows"
        ```python
        from motion import Component

        Calculator = Component("MyCalculator")

        @Calculator.init_state
        def setUp():
            return {"value": 0}

        @Calculator.serve("add")
        def plus(state, props):
            return state["value"] + props["value"]

        @Calculator.serve("subtract")
        def minus(state, props):
            return state["value"] - props["value"]

        @Calculator.update(["add", "subtract"])
        def decrement(state, props):
            return {"value": props.serve_result}

        if __name__ == "__main__":
            c = Calculator()
            c.run("add", props={"value": 1}, flush_update=True) # Will return 1,
            # blocking until update is done. Resulting state is {"value": 1}
            c.run("subtract", props={"value": 1}, flush_update=True)
            # Will return 0, blocking until update is done. Resulting state is #
            # {"value": 0}
        ```

    === "Batching update operations"

        ```python
        from motion import Component
        import numpy as np

        MLMonitor = Component("Monitoring_ML_Component")

        @MLMonitor.init_state
        def setUp():
            return {
                "model": YOUR_MODEL_HERE,
                "historical_values": [],
                "historical_serve_res": []
            }

        @MLMonitor.serve("predict")
        def predict(state, props):
            return state["model"].predict(props["features"])

        @MLMonitor.update("features")
        def monitor(state, props):

            values = state["historical_values"] + [props["features"]]
            serve_results = state["historical_serve_res"] + [props.serve_result]

            # Check drift every 10 values
            if len(values) == 10:
                if YOUR_ANOMALY_ALGORITHM(values, serve_results):
                    # Fire an alert
                    YOUR_ALERT_FUNCTION()
                values = []
                serve_results = []

            return {
                "historical_values": values,
                "historical_serve_res": serve_results
            }

        if __name__ == "__main__":
            c = MLMonitor() # Create instance
            c.run("predict", props={"features": YOUR_FEATURES_HERE})
            # Some alert may be fired in the background!
        ```
    """

    def __init__(
        self,
        name: str,
        params: Dict[str, Any] = {},
        cache_ttl: int = DEFAULT_KEY_TTL,
    ):
        """Creates a new Motion component.

        Args:
            name (str):
                Name of the component.
            params (Dict[str, Any], optional):
                Parameters to be accessed by the component. Defaults to {}.
                Usage: `C.params["param_name"]` if C is the Component you
                have created.
            cache_ttl (int, optional):
                Time to live for cached serve results (seconds).
                Defaults to 1 day. Set to 0 to disable caching.
        """
        if cache_ttl is None or cache_ttl < 0:
            raise ValueError(
                "cache_ttl must be 0 (caching disabled) or a positive integer."
            )

        self._name = name
        self._params = Params(params)
        self._cache_ttl = cache_ttl

        # Set up routes
        self._serve_routes: Dict[str, Route] = {}
        self._update_routes: Dict[str, List[Route]] = {}
        self._init_state_func: Optional[Callable] = None
        self._save_state_func: Optional[Callable] = None
        self._load_state_func: Optional[Callable] = None

    @property
    def cache_ttl(self) -> int:
        """Time to live for cached serve results (seconds)."""
        return self._cache_ttl

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
        if op == "serve":
            if key in self._serve_routes.keys():
                raise ValueError(
                    f"Cannot have more than one serve route for key `{key}`."
                )

            self._serve_routes[key] = Route(key=key, op=op, udf=udf)
        elif op == "update":
            if key not in self._update_routes.keys():
                self._update_routes[key] = []

            self._update_routes[key].append(Route(key=key, op=op, udf=udf))

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

        @MyComponent.serve("add")
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

    def serve(self, keys: Union[str, List[str]]) -> Callable:
        """Decorator for any serve operation for a dataflow through the
        component. Takes in a string or list of strings that represents the
        dataflow key. If the decorator is called with a list of strings, each
        dataflow key will be mapped to the same serve function.

        2 arguments required for an serve operation:
            * `state`: The current state of the component instance, which is a
                dictionary with string keys and any-type values.
            * `props`: The properties of the current flow, which is passed via
                the `run` method of the component instance. You can add to
                the `props` dictionary in the serve op, and the modified
                `props` will be passed to the subsequent update ops in the flow.
                Props are short-lived and die after the dataflow's update op
                finishes.

        Components can have multiple serve ops, but no dataflow key within
        the component can have more than one serve op. serve ops should not
        modify the state object. If you want to modify the state object, write
        an `update` op for your flow.

        Example Usage:
        ```python
        from motion import Component

        MyComponent = Component("MyComponent")

        @MyComponent.init_state
        def setUp():
            return {"value": 0}

        @MyComponent.serve("add")
        def add(state, props):
            return state["value"] + props["value"]

        @MyComponent.serve("multiply")
        def multiply(state, props):
            return state["value"] * props["value"]

        c = MyComponent()
        c.run("add", props={"value": 1}, flush_update=True) # Returns 1
        c.run("multiply", props={"value": 2}) # Returns 2
        ```

        Args:
            keys (Union[str, List[str]]): String or list of strings that
                represent the input keyword(s) for the serve dataflow.

        Returns:
            Callable: Decorated serve function.
        """
        if isinstance(keys, str):
            keys = [keys]

        for key in keys:
            if "::" in key:
                raise ValueError(
                    f"Dataflow key {key} should not have a double colon (::)"
                )

        def decorator(func: Callable) -> Any:
            # type_hint = get_type_hints(func).get("value", None)
            if not validate_args(inspect.signature(func).parameters, "serve"):
                raise ValueError(
                    f"serve function {func.__name__} should have arguments "
                    + "`state` and `props`"
                )

            func._op = "serve"  # type: ignore

            for key in keys:
                self.add_route(key, func._op, func)  # type: ignore

            return func

        return decorator

    def update(self, keys: Union[str, List[str]]) -> Any:
        """Decorator for any update operations for dataflows through the
        component. Takes in a string or list of strings that represents the
        dataflow key. If the decorator is called with a list of strings, each
        dataflow key will be mapped to the same update operation.

        2 arguments required for a update operation:
            - `state`: The current state of the component, represented as a
            dictionary.
            - `props`: The properties of the current flow, which could contain
            properties that were added to the `props` dictionary
            in the serve op before this update op. Props are short-lived and
            die after the dataflow's update op finishes.

        Components can have multiple update ops, and the same key can also have
        multiple update ops. Update functions should return a dictionary
        of state updates to be merged with the current state.

        Example Usage:
        ```python
        from motion import Component

        MyComponent = Component("MyComponent")


        @MyComponent.init_state
        def setUp():
            return {"value": 0}


        @MyComponent.serve("multiply")
        def multiply(state, props):
            props["something"] = props["value"] + 1
            return state["value"] * props["value"]


        @MyComponent.update("multiply")
        def multiply(state, props):
            return {"value": props["something"]}


        if __name__ == "__main__":
            c = MyComponent()
            print(
                c.run("multiply", props={"value": 2}, flush_update=True)
            )  # Returns 0 and state updates to {"value": 3}
            print(
                c.run("multiply", props={"value": 3}, flush_update=True)
            )  # Returns 9, update will execute
            # to get state["value"] = 4
            print(
                c.run("multiply", props={"value": 4}, flush_update=True)
            )  # Returns 4 * 4 = 16
        ```

        Args:
            keys (Union[str, List[str]]): String or list of strings that
                represent the input keyword(s) for the update dataflow.

        Returns:
            Callable: Decorated update function.
        """
        frame = inspect.currentframe().f_back  # type: ignore
        fname = frame.f_code.co_name  # type: ignore
        if fname != "<module>":
            raise ValueError(
                f"Component {self.name} update method must be defined in a module "
                + f"context. It's currently initialized from function {fname}."
            )
        if isinstance(keys, str):
            keys = [keys]

        def decorator(func: Callable) -> Any:
            if not validate_args(inspect.signature(func).parameters, "update"):
                raise ValueError(
                    f"Update op {func.__name__} should have 2 arguments: "
                    + "`state` and `props`."
                )

            # func._input_key = key  # type: ignore
            # func._batch_size = batch_size  # type: ignore
            func._op = "update"  # type: ignore

            for key in keys:
                self.add_route(key, func._op, func)  # type: ignore

            return func

        return decorator

    def __call__(
        self,
        instance_id: str = "",
        init_state_params: Dict[str, Any] = {},
        logging_level: str = "WARNING",
        update_task_type: Literal["thread", "process"] = "thread",
        disable_update_task: bool = False,
        redis_socket_timeout: int = 60,
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

        # Define serve and update operations
        @MyComponent.serve("key1")
        def ...

        @MyComponent.update("key1)
        def ...

        # Creates instance of MyComponent
        if __name__ == "__main__":
            c_instance = MyComponent(init_state_params={"starting_val": 3})
            c_instance.run(..)
        ```

        You can also use component instances as context managers:
        ```python
        ...
        if __name__ == "__main__":
            with MyComponent(init_state_params={"starting_val": 3}) as c_instance:
                c_instance.run(..)
        ```

        Args:
            instance_id (str, optional):
                id of the component instance. Defaults to "" which will
                generate a random id.
            init_state_params (Dict[str, Any], optional):
                Parameters to pass into the init_state function. Defaults to {}.
            logging_level (str, optional):
                Logging level for the Motion logger. Uses the logging library.
                Defaults to "WARNING".
            update_task_type (str, optional):
                Type of update task to use. Can be "thread" or "process".
                "thread" has lower overhead but is not recommended for
                CPU-intensive update operations. "process" is recommended
                for CPU-intensive operations (e.g., fine-tuning a model)
                but has higher startup overhead. Defaults to "thread".
            disable_update_task (bool, optional):
                Whether or not to disable the component instance update ops.
                Useful for printing out state values without running dataflows.
                Defaults to False.
            redis_socket_timeout (int, optional):
                Timeout for redis socket connections (seconds). Defaults to 60.
                This means the redis connection will close if idle for 60 seconds.
        Returns:
            ComponentInstance: Component instance to run dataflows with.
        """
        if not instance_id:
            instance_id = random_passphrase()

        if "__" in instance_id:
            raise ValueError(
                f"Instance name {instance_id} cannot contain '__'. Strip the component"
                + "name from your instance id."
            )

        try:
            ci = ComponentInstance(
                component_name=self.name,
                instance_id=instance_id,
                init_state_func=self._init_state_func,
                init_state_params=init_state_params,
                save_state_func=self._save_state_func,
                load_state_func=self._load_state_func,
                serve_routes=self._serve_routes,
                update_routes=self._update_routes,
                logging_level=logging_level,
                update_task_type=update_task_type,
                disable_update_task=disable_update_task,
                cache_ttl=self._cache_ttl,
                redis_socket_timeout=redis_socket_timeout,
            )
        except RuntimeError:
            raise RuntimeError(
                "Error creating component instance. Make sure the entry point "
                + "of your program is protected, using `if __name__ == '__main__':`"
            )

        return ci

    def get_graph(self, x_offset_step: int = 600) -> Dict[str, Any]:
        """
        Gets the graph of serve and update ops for this component.
        """

        graph: Dict[str, Dict[str, Any]] = {}

        for key, route in self._serve_routes.items():
            graph[key] = {
                "serve": {
                    "name": route.udf.__name__,
                    "udf": inspect.getsource(route.udf),
                },
            }

        for key, routes in self._update_routes.items():
            if key not in graph:
                graph[key] = {}
            graph[key]["update"] = []
            for route in routes:
                graph[key]["update"].append(
                    {
                        "name": route.udf.__name__,
                        "udf": inspect.getsource(route.udf),
                        # "batch_size": route.udf._batch_size,  # type: ignore
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

            # Assign x position for serve nodes
            serve_x_offset = x_offset

            if "serve" in value.keys():
                # Add serve node
                serve_node = {
                    "id": str(node_id),
                    "position": {"x": serve_x_offset, "y": key_y_position},
                    "data": {
                        "label": value["serve"]["name"],
                        "udf": value["serve"]["udf"],
                    },
                    "type": "serve",
                }
                nodes.append(serve_node)
                edges.append(
                    {
                        "id": "e{}-{}".format(key_node["id"], serve_node["id"]),
                        "source": key_node["id"],
                        "target": serve_node["id"],
                        "targetHandle": "left",
                    }
                )
                edges.append(
                    {
                        "id": "e{}-{}".format(state_node["id"], serve_node["id"]),
                        "source": state_node["id"],
                        "target": serve_node["id"],
                        "targetHandle": "top",
                    }
                )
                node_id += 1

            # Assign x position for update nodes
            serve_x_offset += x_offset_step
            fit_x_offset = serve_x_offset

            if "update" in value.keys():
                for update in value["update"]:
                    # Add update node
                    fit_node = {
                        "id": str(node_id),
                        "position": {"x": fit_x_offset, "y": key_y_position},
                        "data": {
                            "label": update["name"],
                            "udf": update["udf"],
                            # "batch_size": fit["batch_size"],
                        },
                        "type": "update",
                    }
                    nodes.append(fit_node)

                    edges.append(
                        {
                            "id": "e{}-{}".format(fit_node["id"], state_node["id"]),
                            "target": state_node["id"],
                            "source": fit_node["id"],
                            "sourceHandle": "top",
                            "animated": True,  # type: ignore
                            # "label": f"batch_size: {fit['batch_size']}",
                        }
                    )

                    if "serve" in value.keys():
                        edges.append(
                            {
                                "id": "e{}-{}".format(serve_node["id"], fit_node["id"]),
                                "source": serve_node["id"],
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
