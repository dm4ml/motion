import asyncio
import multiprocessing
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple
from uuid import uuid4

import cloudpickle
import psutil
import redis
from redis.lock import Lock

from motion.route import Route
from motion.server.fit_task import FitTask
from motion.utils import (
    CustomDict,
    FitEvent,
    FitEventGroup,
    RedisParams,
    hash_object,
    loadState,
    logger,
    saveState,
)


class Executor:
    def __init__(
        self,
        instance_name: str,
        init_state_func: Optional[Callable],
        init_state_params: Dict[str, Any],
        save_state_func: Optional[Callable],
        load_state_func: Optional[Callable],
        infer_routes: Dict[str, Route],
        fit_routes: Dict[str, List[Route]],
    ):
        self._instance_name = instance_name

        self._init_state_func = init_state_func
        self._init_state_params = init_state_params
        self._load_state_func = load_state_func
        self._save_state_func = save_state_func

        self.running: Any = multiprocessing.Value("b", False)
        self._redis_con = self._connectToRedis()
        try:
            self._redis_con.ping()
        except redis.exceptions.ConnectionError:
            rp = RedisParams()
            raise ConnectionError(
                f"Could not connect to a Redis backend {rp}. "
                + "Please set environment variables MOTION_REDIS_HOST, "
                + "MOTION_REDIS_PORT, MOTION_REDIS_DB, and/or "
                + "MOTION_REDIS_PASSWORD to your Redis params."
            )
        self.running.value = True

        # Set up state
        self.version = self._redis_con.get(f"MOTION_VERSION:{self._instance_name}")
        self._state = CustomDict(
            instance_name.split("__")[0],
            "state",
            instance_name.split("__")[1],
            {},
        )
        if self.version is None:
            self.version = 1
            # Setup state
            self._state.update(self.setUp(**self._init_state_params))
            saveState(
                self._state,
                self._redis_con,
                self._instance_name,
                self._save_state_func,
            )
        else:
            # Load state
            self._state = self._loadState()
            self.version = self._redis_con.get(f"MOTION_VERSION:{self._instance_name}")

        self.version = int(self.version)

        # Set up routes
        self._infer_routes: Dict[str, Route] = infer_routes
        self._fit_routes: Dict[str, Dict[str, Route]] = {
            rkey: {route.udf.__name__: route for route in routes}
            for rkey, routes in fit_routes.items()
        }

        # Set up shutdown event
        # self._shutdown_event = threading.Event()

        # Set up fit queues, batch sizes, and threads
        self._build_fit_jobs()

    def _connectToRedis(self) -> redis.Redis:
        rp = RedisParams()
        r = redis.Redis(
            host=rp.host,
            port=rp.port,
            password=rp.password,
            db=rp.db,
        )
        return r

    def _loadState(self) -> CustomDict:
        return loadState(self._redis_con, self._instance_name, self._load_state_func)

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
        # Set up worker loops
        self.worker_tasks = {}
        # self.worker_stop_events = {}

        rp = RedisParams()
        # self.worker_states = {}

        for rkey, routes in self._fit_routes.items():
            for udf_name, route in routes.items():
                pname = f"{self._instance_name}::{rkey}::{udf_name}"
                multiprocessing.Event()
                self.worker_tasks[pname] = FitTask(
                    self._instance_name,
                    route,
                    batch_size=route.udf._batch_size,  # type: ignore
                    save_state_func=self._save_state_func,
                    load_state_func=self._load_state_func,
                    queue_identifier=self._get_queue_identifier(rkey, udf_name),
                    channel_identifier=self._get_channel_identifier(rkey, udf_name),
                    redis_host=rp.host,
                    redis_port=rp.port,
                    redis_db=rp.db,
                    redis_password=rp.password,  # type: ignore
                    running=self.running,
                )
                # self.worker_stop_events[pname] = worker_stop_event
                self.worker_tasks[pname].start()

        # Set up a monitor thread
        self.stop_event = threading.Event()
        self.monitor_thread = threading.Thread(
            target=self._monitor_processes, daemon=True
        )
        self.monitor_thread.start()

    def _monitor_processes(self) -> None:
        rp = RedisParams()
        while not self.stop_event.is_set():
            # Loop through processes to see if they are alive
            for pname, process in self.worker_tasks.items():
                if not process.is_alive():
                    logger.debug(
                        f"Failed to detect heartbeat for fit task {pname}."
                        + " Restarting the task in the background."
                    )
                    # Restart
                    rkey = pname.split("::")[1]
                    udf_name = pname.split("::")[2]
                    route = self._fit_routes[rkey][udf_name]
                    self.worker_tasks[pname] = FitTask(
                        self._instance_name,
                        route,
                        batch_size=route.udf._batch_size,  # type: ignore
                        save_state_func=self._save_state_func,
                        load_state_func=self._load_state_func,
                        queue_identifier=self._get_queue_identifier(rkey, udf_name),
                        channel_identifier=self._get_channel_identifier(rkey, udf_name),
                        redis_host=rp.host,
                        redis_port=rp.port,
                        redis_db=rp.db,
                        redis_password=rp.password,  # type: ignore
                        running=self.running,
                    )
                    self.worker_tasks[pname].start()

            if self.stop_event.is_set():
                break

            # Sleep for a minute
            self.stop_event.wait(60)

    def _get_queue_identifier(self, route_key: str, udf_name: str) -> str:
        """Gets the queue identifier for a given route key and UDF name."""
        return f"MOTION_QUEUE:{self._instance_name}/{route_key}/{udf_name}"

    def _get_channel_identifier(self, route_key: str, udf_name: str) -> str:
        """Gets the channel identifier for a given route key and UDF name."""
        return f"MOTION_CHANNEL:{self._instance_name}/{route_key}/{udf_name}"

    def shutdown(self, is_open: bool) -> None:
        if not self.running.value:
            return

        if is_open:
            logger.info("Running fit operations on remaining data...")

        # Set shutdown event
        self.stop_event.set()
        self.running.value = False

        processes_to_wait_for = []
        for process in self.worker_tasks.values():
            if psutil.pid_exists(process.pid):
                # os.kill(process.pid, signal.SIGUSR1)  # type:ignore
                # Set stop event
                # self.worker_stop_events[pname].set()
                processes_to_wait_for.append(process)

        self._redis_con.close()

        # Join fit processes
        for process in processes_to_wait_for:
            process.join()

        self.monitor_thread.join()

    def _updateState(self, new_state: Dict[str, Any]) -> None:
        if not new_state:
            return

        if not isinstance(new_state, dict):
            raise TypeError("State should be a dict.")

        # Acquire a lock
        lock_timeout = 5  # Lock timeout in seconds
        lock = Lock(self._redis_con, self._instance_name, lock_timeout)

        acquired_lock = lock.acquire(blocking=True)
        if acquired_lock:
            self._state.update(new_state)

            # Get latest state
            self._state = self._loadState()
            self._state.update(new_state)

            # Save state to redis
            saveState(
                self._state,
                self._redis_con,
                self._instance_name,
                self._save_state_func,
            )

            self.version = self._redis_con.get(f"MOTION_VERSION:{self._instance_name}")

            # Release lock
            lock.release()

    def empty_batch(self) -> Dict[str, List[Any]]:
        return {
            "fit_events": [],
            "values": [],
            "infer_results": [],
        }

    def _enqueue_and_trigger_fit(
        self,
        key: str,
        value: Any,
        infer_result: Any,
        flush_fit: bool,
        route_hit: bool,
    ) -> bool:
        # Run the fit routes
        # Enqueue results into fit queues
        if key in self._fit_routes.keys():
            route_hit = True

            fit_events = FitEventGroup(key)
            for fit_udf_name in self._fit_routes[key].keys():
                queue_identifier: str = self._get_queue_identifier(key, fit_udf_name)
                channel_identifier: str = self._get_channel_identifier(
                    key, fit_udf_name
                )

                identifier = str(uuid4())

                if flush_fit:
                    # Add pubsub channel to listen to
                    fit_event = FitEvent(
                        self._redis_con, channel_identifier, identifier
                    )
                    fit_events.add(fit_udf_name, fit_event)

                # Add to fit queue
                self._redis_con.rpush(
                    queue_identifier,
                    cloudpickle.dumps(
                        (
                            {
                                "value": value,
                                "infer_result": infer_result,
                                "identifier": identifier,
                            },
                            flush_fit,
                        )
                    ),
                )

            if flush_fit:
                # Wait for fit result to finish
                fit_events.wait()
                # Update state
                self._state = self._loadState()
                v = self._redis_con.get(f"MOTION_VERSION:{self._instance_name}")
                if not v:
                    raise ValueError(
                        f"Error loading state for {self._instance_name}."
                        + " No version found."
                    )
                self.version = int(v)

        return route_hit

    def _try_cached_infer(
        self,
        key: str,
        value: Any,
        ignore_cache: bool,
        force_refresh: bool,
    ) -> Tuple[bool, Optional[Any], Optional[str]]:
        route_run = False
        infer_result = None

        if force_refresh:
            self._state = self._loadState()
            v = self._redis_con.get(f"MOTION_VERSION:{self._instance_name}")
            if not v:
                raise ValueError(
                    f"Error loading state for {self._instance_name}."
                    + " No version found."
                )
            self.version = int(v)

        # Try hashing the value
        try:
            value_hash = hash_object(value)
        except TypeError:
            value_hash = None

        # Check if key is in cache if value can be hashed and
        # user doesn't want to force refresh state
        if value_hash and not force_refresh and not ignore_cache:
            cache_result_key = f"MOTION_RESULT:{self._instance_name}/{key}/{value_hash}"
            if self._redis_con.exists(cache_result_key):
                infer_result = cloudpickle.loads(self._redis_con.get(cache_result_key))
                route_run = True

        return route_run, infer_result, value_hash

    def run(
        self,
        key: str,
        value: Any,
        cache_ttl: int,
        ignore_cache: bool,
        force_refresh: bool,
        flush_fit: bool,
    ) -> Any:
        route_hit = False
        infer_result = None

        # Run the infer route
        if key in self._infer_routes.keys():
            route_hit = True
            route_run, infer_result, value_hash = self._try_cached_infer(
                key, value, ignore_cache, force_refresh
            )

            # If not in cache or value can't be hashed or
            # user wants to force refresh state, run route
            if not route_run:
                infer_result = self._infer_routes[key].run(
                    state=self._state, value=value
                )

                # Check that infer_result is not an awaitable
                if asyncio.iscoroutine(infer_result):
                    raise TypeError(
                        f"Route {key} returned an awaitable. "
                        + "Call `await instance.arun(...)` instead."
                    )

                # Cache result
                if value_hash:
                    cache_result_key = (
                        f"MOTION_RESULT:{self._instance_name}/{key}/{value_hash}"
                    )
                    self._redis_con.set(
                        cache_result_key,
                        cloudpickle.dumps(infer_result),
                        ex=cache_ttl,
                    )

        # Run the fit routes
        # Enqueue results into fit queues
        route_hit = self._enqueue_and_trigger_fit(
            key, value, infer_result, flush_fit, route_hit
        )

        if not route_hit:
            raise KeyError(f"Key {key} not in routes.")

        return infer_result

    async def arun(
        self,
        key: str,
        value: Any,
        cache_ttl: int,
        ignore_cache: bool,
        force_refresh: bool,
        flush_fit: bool,
    ) -> Any:
        route_hit = False
        infer_result = None

        # Run the infer route
        if key in self._infer_routes.keys():
            route_hit = True
            route_run, infer_result, value_hash = self._try_cached_infer(
                key, value, ignore_cache, force_refresh
            )

            # If not in cache or value can't be hashed or
            # user wants to force refresh state, run route
            if not route_run:
                infer_result_awaitable = self._infer_routes[key].run(
                    state=self._state, value=value
                )
                if not asyncio.iscoroutine(infer_result_awaitable):
                    raise TypeError(
                        f"Route {key} returned a non-awaitable. "
                        + "Call `instance.run(...)` instead."
                    )

                infer_result = await infer_result_awaitable

                # Cache result
                if value_hash:
                    cache_result_key = (
                        f"MOTION_RESULT:{self._instance_name}/{key}/{value_hash}"
                    )
                    self._redis_con.set(
                        cache_result_key,
                        cloudpickle.dumps(infer_result),
                        ex=cache_ttl,
                    )

        # Run the fit routes
        # Enqueue results into fit queues
        route_hit = self._enqueue_and_trigger_fit(
            key, value, infer_result, flush_fit, route_hit
        )

        if not route_hit:
            raise KeyError(f"Key {key} not in routes.")

        return infer_result

    def flush_fit(self, dataflow_key: str) -> None:
        # Check if key has fit ops
        if dataflow_key not in self._fit_routes.keys():
            return

        # Push a noop into the relevant queues
        fit_events = FitEventGroup(dataflow_key)
        for fit_udf_name in self._fit_routes[dataflow_key].keys():
            queue_identifier: str = self._get_queue_identifier(
                dataflow_key, fit_udf_name
            )
            channel_identifier: str = self._get_channel_identifier(
                dataflow_key, fit_udf_name
            )

            identifier = "NOOP_" + str(uuid4())

            # Add pubsub channel to listen to
            fit_event = FitEvent(self._redis_con, channel_identifier, identifier)
            fit_events.add(fit_udf_name, fit_event)

            # Add to fit queue
            self._redis_con.rpush(
                queue_identifier,
                cloudpickle.dumps(
                    (
                        {
                            "value": None,
                            "infer_result": None,
                            "identifier": identifier,
                        },
                        True,
                    )
                ),
            )

        # Wait for fit result to finish
        fit_events.wait()
        # Update state
        self._state = self._loadState()
        v = self._redis_con.get(f"MOTION_VERSION:{self._instance_name}")
        if not v:
            raise ValueError(
                f"Error loading state for {self._instance_name}." + " No version found."
            )

        self.version = int(v)
