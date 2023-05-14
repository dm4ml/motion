import threading
from queue import Empty, SimpleQueue
from typing import Any, Callable, Dict, List, Optional, Tuple

from motion.route import Route
from motion.utils import CustomDict, FitEventGroup, logger


class Executor:
    def __init__(
        self,
        instance_name: str,
        init_state_func: Optional[Callable],
        infer_routes: Dict[str, Route],
        fit_routes: Dict[str, List[Route]],
        cleanup: bool,
    ):
        self._instance_name = instance_name
        self._cleanup = cleanup
        self._init_state_func = init_state_func

        # Set up state
        self.setUp()

        # Set up routes
        self._infer_routes: Dict[str, Route] = infer_routes
        self._fit_routes: Dict[str, List[Route]] = fit_routes

        # Set up shutdown event
        self._shutdown_event = threading.Event()

        # Set up fit queues, batch sizes, and threads
        self._build_fit_threads()

    def _build_fit_threads(self) -> None:
        """Builds fit queues, with batch sizes and threads to run them."""
        self._fit_queues: Dict[str, Dict[str, SimpleQueue]] = {}
        self._batch_sizes: Dict[str, Dict[str, int]] = {}
        self._fit_threads: Dict[str, Dict[str, threading.Thread]] = {}

        for key, routes in self._fit_routes.items():
            self._fit_queues[key] = {}
            self._batch_sizes[key] = {}
            self._fit_threads[key] = {}

            for route in routes:
                udf = route.udf
                uname = route.udf.__name__
                self._fit_queues[key][uname] = SimpleQueue()
                self._batch_sizes[key][uname] = udf._batch_size  # type: ignore
                self._fit_threads[key][uname] = threading.Thread(
                    target=self.processFitQueue,
                    args=(key, uname),
                    daemon=True,
                    name=f"{self._instance_name}_{key}_{uname}_fit",
                )
                self._fit_threads[key][uname].start()

    def setUp(self) -> None:
        # Set up initial state
        self._state = CustomDict(self._instance_name, "state", {})
        if self._init_state_func is not None:
            initial_state = self._init_state_func()
            if not isinstance(initial_state, dict):
                raise TypeError(f"{self._instance_name} init should return a dict.")
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
