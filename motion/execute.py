import os
import threading
from queue import Empty, SimpleQueue
from typing import Any, Callable, Dict, List, Optional, Tuple

import cloudpickle
import redis

from motion.route import Route
from motion.utils import CustomDict, FitEventGroup, logger


class Executor:
    def __init__(
        self,
        instance_name: str,
        init_state_func: Optional[Callable],
        init_state_params: Optional[Dict[str, Any]],
        save_state_func: Optional[Callable],
        load_state_func: Optional[Callable],
        infer_routes: Dict[str, Route],
        fit_routes: Dict[str, List[Route]],
        cleanup: bool,
        redis_con: Optional[redis.Redis] = None,
    ):
        self._instance_name = instance_name
        self._cleanup = cleanup

        self._init_state_func = init_state_func
        self._load_state_func = load_state_func
        self._save_state_func = save_state_func

        self._redis_con = self._connectToRedis() if redis_con is None else redis_con

        # Set up state
        empty_state = CustomDict(self._instance_name, "state", {})
        initial_state = self.loadState(empty_state, **init_state_params)
        self._state = initial_state

        # Set up routes
        self._infer_routes: Dict[str, Route] = infer_routes
        self._fit_routes = fit_routes
        # self._fit_routes: Dict[str, Dict[str, Route]] = {
        #     rkey: {route.udf.__name__: route for route in routes}
        #     for rkey, routes in fit_routes.items()
        # }

        # Set up shutdown event
        self._shutdown_event = threading.Event()

        # Set up fit queues, batch sizes, and threads
        self._build_fit_threads()

        # self._build_fit_jobs()

    def _connectToRedis(self) -> redis.Redis:
        host = os.getenv("MOTION_REDIS_HOST", "localhost")
        port = os.getenv("MOTION_REDIS_PORT", 6379)
        password = os.getenv("MOTION_REDIS_PASSWORD", None)

        r = redis.Redis(
            host=host,
            port=port,
            password=password,
        )
        return r

    def loadState(self, state: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        # Get state from redis
        loaded_state = self._redis_con.get(f"MOTION_STATE:{self._instance_name}")

        if not loaded_state:
            # Set up initial state
            return self.setUp(**kwargs)

        # Unpickle state
        loaded_state = cloudpickle.loads(loaded_state)

        if self._load_state_func is not None:
            state.update(self._load_state_func(loaded_state))
        else:
            state.update(loaded_state)

        return state

    def saveState(self, state_to_save: Dict[str, Any]) -> None:
        # Save state to redis
        if self._save_state_func is not None:
            state_to_save = self._save_state_func(state_to_save)

        state_to_save = cloudpickle.dumps(state_to_save)

        self._redis_con.set(f"MOTION_STATE:{self._instance_name}", state_to_save)

    def setUp(self, **kwargs: Any) -> Dict[str, Any]:
        # Set up initial state
        if self._init_state_func is not None:
            initial_state = self._init_state_func(**kwargs)
            if not isinstance(initial_state, dict):
                raise TypeError(f"{self._instance_name} init should return a dict.")
            return initial_state

        return {}

    def _build_fit_jobs(self) -> None:
        """Builds fit jobs."""
        pass

    def _get_queue_identifier(self, route_key: str, udf_name: str) -> str:
        """Gets the queue identifier for a given route key and UDF name."""
        return f"MOTION_QUEUE:{self._instance_name}/{route_key}/{udf_name}"

    async def worker_loop(
        self,
        route_key: str,
        udf_name: str,
        batch_size: int,
    ):
        """Worker loop for processing fit jobs."""
        queue_identifier: str = self._get_queue_identifier(route_key, udf_name)
        route = self._fit_routes[route_key][udf_name]

        while True:
            job_data = self._redis_con.blmpop(
                0,
                1,
                queue_identifier,
                direction="LEFT",
                count=batch_size,
            )

            # Execute fit job
            if job_data:
                job_data = job_data[0][1]
                job_data = [cloudpickle.loads(job) for job in job_data]
                values = [job["value"] for job in job_data]
                infer_results = [job["infer_results"] for job in job_data]

                new_state = route.run(
                    state=self.state,
                    values=values,
                    infer_results=infer_results,
                )

                if not isinstance(new_state, dict):
                    # fit_event.set()
                    raise TypeError(
                        "fit methods should return a dict of state updates."
                    )

                self.update(new_state)

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

    def shutdown(self, is_open: bool) -> None:
        if self._cleanup and is_open:
            logger.info("Running fit operations on remaining data...")

        # Set shutdown event
        self._shutdown_event.set()

        # Join fit threads
        for _, val in self._fit_threads.items():
            for v in val.values():
                v.join()

        # Save state
        if is_open:
            logger.info(f"Saving state from {self._instance_name}...")

        self.saveState(self.state)

        if is_open:
            logger.info(f"Finished shutting down {self._instance_name}.")

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
