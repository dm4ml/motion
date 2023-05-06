import threading
from queue import SimpleQueue
from typing import Any, Dict, List, Tuple

from motion.compiler import RouteCompiler


class Executor:
    def __init__(self, component: object):
        self.component = component
        self.component_name = component.__class__.__name__

        # Set up initial state
        self._state = CustomDict(self.component_name, "state", {})
        initial_state = self.component.setUp()
        if not isinstance(initial_state, dict):
            raise TypeError(f"{self.component_name} setUp() should return a dict.")
        self.update(initial_state)

        # Build routes and fit queue
        self.build()

    def build(self):
        rc = RouteCompiler(self.component)
        (
            self.infer_routes,
            self.fit_routes,
        ) = rc.compile_routes()

        # Set up fit queues
        fit_methods = rc.get_decorated_methods("fit")
        self.fit_queues = {getattr(m, "_input_key"): SimpleQueue() for m in fit_methods}
        self.batch_sizes = {
            getattr(m, "_input_key"): getattr(m, "_batch_size") for m in fit_methods
        }

        self.fit_threads = {}
        for m in fit_methods:
            key = getattr(m, "_input_key")
            self.fit_threads[key] = threading.Thread(
                target=self.processFitQueue,
                args=(key,),
                daemon=True,
                name=f"{self.component_name}_{key}_fit",
            )
        for t in self.fit_threads.values():
            t.start()

    def shutdown(self):
        for t in self.fit_threads.values():
            t.join()

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

    def processFitQueue(self, route_key: str):
        while True:
            batch = self.empty_batch()

            for _ in range(self.batch_sizes[route_key]):
                fit_event, value, infer_result = self.fit_queues[route_key].get()
                batch["fit_events"].append(fit_event)
                batch["values"].append(value)
                batch["infer_results"].append(infer_result)

            new_state = self.fit_routes[route_key].run(
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

    def run(self, **kwargs: Dict[str, Any]) -> Tuple[Any, threading.Event]:
        if len(kwargs) != 1:
            raise ValueError("Only one key-value pair is allowed in kwargs.")

        key, value = next(iter(kwargs.items()))
        route_hit = False
        infer_result = None

        # Run the infer route
        if key in self.infer_routes.keys():
            route_hit = True
            infer_result = self.infer_routes[key].run(state=self.state, value=value)

        # Run the fit route
        if key in self.fit_routes.keys():
            route_hit = True
            fit_event = threading.Event()
            self.fit_queues[key].put((fit_event, value, infer_result))
            return infer_result, fit_event

        if not route_hit:
            raise KeyError(f"Key {key} not in routes.")

        return infer_result, None


class CustomDict(dict):
    def __init__(
        self,
        component_name: str,
        dict_type: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.component_name = component_name
        self.dict_type = dict_type
        super().__init__(*args, **kwargs)

    def __getitem__(self, key: str) -> object:
        try:
            return super().__getitem__(key)
        except KeyError:
            raise KeyError(
                f"Key `{key}` not found in {self.dict_type} for "
                + f"component {self.component_name}."
            )
