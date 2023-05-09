import inspect
import threading
from queue import Empty, SimpleQueue
from typing import Any, Callable, Dict, List, Optional, Tuple

from motion.route import Route
from motion.utils import CustomDict, FitEventGroup, logger


class Executor:
    def __init__(self, component_name: str, cleanup: bool):
        self._component_name = component_name
        self._cleanup = cleanup
        self._first_run = True  # Use this to determine whether to run setUp
        self._init_state_func: Optional[Callable] = None

        # Set up routes
        self._infer_routes: Dict[str, Route] = {}
        self._fit_routes: Dict[str, List[Route]] = {}

        # Set up shutdown event
        self._shutdown_event = threading.Event()

        # Set up fit queues, batch sizes, and threads
        self._fit_queues: Dict[str, Dict[str, SimpleQueue]] = {}
        self._batch_sizes: Dict[str, Dict[str, int]] = {}
        self._fit_threads: Dict[str, Dict[str, threading.Thread]] = {}

    @property
    def init_state_func(self) -> Optional[Callable]:
        return self._init_state_func

    @init_state_func.setter
    def init_state_func(self, func: Callable) -> None:
        self._init_state_func = func

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
                self._fit_queues[key] = {}
                self._batch_sizes[key] = {}
                self._fit_threads[key] = {}

            uname = udf.__name__
            self._fit_routes[key].append(Route(key=key, op=op, udf=udf))
            self._fit_queues[key][uname] = SimpleQueue()
            self._batch_sizes[key][uname] = udf._batch_size  # type: ignore
            self._fit_threads[key][uname] = threading.Thread(
                target=self.processFitQueue,
                args=(key, uname),
                daemon=True,
                name=f"{self._component_name}_{key}_{uname}_fit",
            )
            self._fit_threads[key][uname].start()

        else:
            raise ValueError(f"Invalid op `{op}`.")

    def setUp(self) -> None:
        # Set up initial state
        self._state = CustomDict(self._component_name, "state", {})
        if self._init_state_func is not None:
            initial_state = self._init_state_func()
            if not isinstance(initial_state, dict):
                raise TypeError(f"{self._component_name} setUp() should return a dict.")
            self.update(initial_state)

    def shutdown(self, is_open: bool) -> None:
        if self._cleanup and is_open:
            logger.info("Running fit operations on remaining data...")

        # Set shutdown event
        self._shutdown_event.set()

        # Join fit threads
        for _, val in self._fit_threads.items():
            for v in val.values():
                v.join()

    @property
    def state(self) -> Dict[str, Any]:
        return self._state

    def update(self, new_state: Dict[str, Any]) -> None:
        if new_state:
            self._state.update(new_state)

    def empty_batch(self) -> Dict[str, List[Any]]:
        return {
            "fit_events": [],
            "values": [],
            "infer_results": [],
        }

    def processFitQueue(self, route_key: str, udf_name: str) -> None:
        while not self._shutdown_event.is_set():
            batch = self.empty_batch()

            num_elements = 0
            while num_elements < self._batch_sizes[route_key][udf_name]:
                try:
                    result = self._fit_queues[route_key][udf_name].get(timeout=1)
                except Empty:
                    # Handle empty queue and check shutdown event again
                    if self._shutdown_event.is_set():
                        if self._cleanup:
                            break  # Break out of while loop and run udf
                        else:
                            return  # Exit function so thread can be joined
                    else:
                        continue

                fit_event, route, value, infer_result = result
                batch["fit_events"].append(fit_event)
                batch["values"].append(value)
                batch["infer_results"].append(infer_result)
                num_elements += 1

            new_state = route.run(
                state=self.state,
                values=batch["values"],
                infer_results=batch["infer_results"],
            )
            if not isinstance(new_state, dict):
                fit_event.set()
                raise TypeError("fit methods should return a dict of state updates.")

            self.update(new_state)

            for fit_event in batch["fit_events"]:
                fit_event.set()

    def run(self, **kwargs: Dict[str, Any]) -> Tuple[Any, Optional[FitEventGroup]]:
        if len(kwargs) != 1:
            raise ValueError("Only one key-value pair is allowed in kwargs.")

        key, value = next(iter(kwargs.items()))
        route_hit = False
        infer_result = None

        if self._first_run:
            # Set up state
            logger.info(f"Setting up {self._component_name} state for the first run...")
            self.setUp()
            logger.info(f"Finished setting up {self._component_name} state.")
            self._first_run = False

        # Run the infer route
        if key in self._infer_routes.keys():
            route_hit = True
            infer_result = self._infer_routes[key].run(state=self.state, value=value)

        # Run the fit routes
        if key in self._fit_routes.keys():
            route_hit = True

            fit_events = FitEventGroup(key)
            for fit in self._fit_routes[key]:
                fit_event = threading.Event()
                self._fit_queues[key][fit.udf.__name__].put(
                    (fit_event, fit, value, infer_result)
                )
                fit_events.add(fit.udf.__name__, fit_event)

            return infer_result, fit_events

        if not route_hit:
            raise KeyError(f"Key {key} not in routes.")

        return infer_result, None

    def get_graph(self, x_offset_step: int = 600) -> Dict[str, Any]:
        """Gets the graph of the component."""

        graph = {}

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
                infer_x_offset += x_offset_step
                node_id += 1

            # Assign x position for fit nodes
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
                            "animated": True,
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
                                "animated": True,
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
        state_node["position"]["x"] = int(max_x_offset / 2)
        nodes.append(state_node)

        return {"name": self._component_name, "nodes": nodes, "edges": edges}
