import asyncio
import logging
import multiprocessing
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple
from uuid import uuid4

import cloudpickle
import psutil
import redis

from motion.dicts import Properties, State
from motion.route import Route
from motion.server.update_task import UpdateProcess, UpdateThread
from motion.utils import (
    RedisParams,
    UpdateEvent,
    UpdateEventGroup,
    get_redis_params,
    hash_object,
    loadState,
    saveState,
)

logger = logging.getLogger(__name__)


class Executor:
    def __init__(
        self,
        instance_name: str,
        cache_ttl: int,
        init_state_func: Optional[Callable],
        init_state_params: Dict[str, Any],
        save_state_func: Optional[Callable],
        load_state_func: Optional[Callable],
        serve_routes: Dict[str, Route],
        update_routes: Dict[str, List[Route]],
        update_task_type: Literal["thread", "process"] = "thread",
        disable_update_task: bool = False,
        redis_socket_timeout: int = 60,
    ):
        self._instance_name = instance_name
        self._cache_ttl = cache_ttl

        self._init_state_func = init_state_func
        self._init_state_params = init_state_params
        self._load_state_func = load_state_func
        self._save_state_func = save_state_func

        self.running: Any = multiprocessing.Value("b", False)
        self._redis_socket_timeout = redis_socket_timeout

        self._redis_params, self._redis_con = self._connectToRedis()
        try:
            self._redis_con.ping()
        except redis.exceptions.ConnectionError:
            raise ConnectionError(
                f"Could not connect to a Redis backend {self._redis_params}. "
                + "Please set environment variables MOTION_REDIS_HOST, "
                + "MOTION_REDIS_PORT, MOTION_REDIS_DB, and/or "
                + "MOTION_REDIS_PASSWORD to your Redis params."
            )
        self.running.value = True

        # Set up state
        self.version = self._redis_con.get(f"MOTION_VERSION:{self._instance_name}")
        self._state = State(
            instance_name.split("__")[0],
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
            self.version = -1  # will get updated in _loadState
            self._loadState()

        # Set up routes
        self._serve_routes: Dict[str, Route] = serve_routes
        self._update_routes: Dict[str, Dict[str, Route]] = {
            rkey: {route.udf.__name__: route for route in routes}
            for rkey, routes in update_routes.items()
        }

        # Set up shutdown event
        # self._shutdown_event = threading.Event()

        # Set up update queues, batch sizes, and threads
        self.disable_update_task = disable_update_task
        if not disable_update_task:
            self.update_task_type = update_task_type
            self._build_fit_jobs()

        self.tp = ThreadPoolExecutor(max_workers=2)

    def _setRedis(self, cache_result_key: str, props: Any) -> None:
        """Method to set value in Redis."""
        self._redis_con.set(
            cache_result_key, cloudpickle.dumps(props), ex=self._cache_ttl
        )

    def _connectToRedis(self) -> Tuple[RedisParams, redis.Redis]:
        rp = get_redis_params()

        # Put a timeout on the connection
        param_dict = rp.dict()
        if "socket_timeout" not in param_dict:
            param_dict["socket_timeout"] = self._redis_socket_timeout

        r = redis.Redis(**param_dict)
        return rp, r

    def _loadState(self) -> None:
        redis_v = self._redis_con.get(f"MOTION_VERSION:{self._instance_name}")
        if not redis_v:
            raise ValueError(
                f"Error loading state for {self._instance_name}." + " No version found."
            )

        if self.version and self.version < int(redis_v):
            # Reload state
            self._state = loadState(
                self._redis_con, self._instance_name, self._load_state_func
            )
            self.version = int(redis_v)

    def setUp(self, **kwargs: Any) -> Dict[str, Any]:
        # Set up initial state
        if self._init_state_func is not None:
            initial_state = self._init_state_func(**kwargs)
            if not isinstance(initial_state, dict):
                raise TypeError(f"{self._instance_name} init should return a dict.")
            return initial_state

        return {}

    def _build_fit_jobs(self) -> None:
        """Builds update job."""

        update_cls = (
            UpdateProcess if self.update_task_type == "process" else UpdateThread
        )

        # Set up update task
        self.route_dict_for_fit = {}
        self.channel_dict_for_fit = {}
        self.queue_ids_for_fit = []
        for rkey, routes in self._update_routes.items():
            for udf_name, route in routes.items():
                queue_id = self._get_queue_identifier(rkey, udf_name)
                self.queue_ids_for_fit.append(queue_id)
                self.route_dict_for_fit[queue_id] = route
                self.channel_dict_for_fit[queue_id] = self._get_channel_identifier(
                    rkey, udf_name
                )

        self.worker_task = None
        if self.queue_ids_for_fit:
            self.worker_task = update_cls(
                instance_name=self._instance_name,
                routes=self.route_dict_for_fit,
                save_state_func=self._save_state_func,
                load_state_func=self._load_state_func,
                queue_identifiers=self.queue_ids_for_fit,
                channel_identifiers=self.channel_dict_for_fit,
                redis_params=self._redis_params.dict(),
                running=self.running,
            )
            self.worker_task.start()  # type: ignore

        # Set up a monitor thread
        self.stop_event = threading.Event()
        self.monitor_thread = threading.Thread(
            target=self._monitor_process, daemon=True
        )
        self.monitor_thread.start()

    def _monitor_process(self) -> None:
        if not self.worker_task:
            return

        update_cls = (
            UpdateProcess if self.update_task_type == "process" else UpdateThread
        )

        while not self.stop_event.is_set():
            # See if the update task is alive
            if not self.worker_task.is_alive():  # type: ignore
                logger.debug(
                    f"No heartbeat for {self.worker_task.name}."  # type: ignore
                    + " Restarting the task in the background."
                )  # type: ignore

                # Restart
                self.worker_task = update_cls(
                    instance_name=self._instance_name,
                    routes=self.route_dict_for_fit,
                    save_state_func=self._save_state_func,
                    load_state_func=self._load_state_func,
                    queue_identifiers=self.queue_ids_for_fit,
                    channel_identifiers=self.channel_dict_for_fit,
                    redis_params=self._redis_params.dict(),
                    running=self.running,
                )
                self.worker_task.start()  # type: ignore

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
        if self.disable_update_task:
            if self._redis_con:
                self._redis_con.close()
            return

        if not self.running.value:
            if self._redis_con:
                self._redis_con.close()
            return

        if is_open:
            logger.debug("Running update operations on remaining data...")

        # Set shutdown event
        self.stop_event.set()
        self.running.value = False

        # If process, check if pid exists
        if self.update_task_type == "process":
            if self.worker_task:
                if psutil.pid_exists(self.worker_task.pid):  # type: ignore
                    self.worker_task.join()  # type: ignore
        # If thread, check if thread is alive
        else:
            if self.worker_task and self.worker_task.is_alive():  # type: ignore
                self.worker_task.join()  # type: ignore

        # Shut down threadpool for writing to Redis
        self.tp.shutdown(wait=False)

        self._redis_con.close()

        self.monitor_thread.join()

    def _updateState(
        self,
        new_state: Dict[str, Any],
        force_update: bool = True,
        use_lock: bool = True,
    ) -> None:
        if not new_state:
            return

        if not isinstance(new_state, dict):
            raise TypeError("State should be a dict.")

        # Get latest state
        if use_lock:
            with self._redis_con.lock(
                f"MOTION_LOCK:{self._instance_name}", timeout=120
            ):
                if force_update:
                    self._loadState()
                self._state.update(new_state)

                # Save state to redis
                saveState(
                    self._state,
                    self._redis_con,
                    self._instance_name,
                    self._save_state_func,
                )

                version = self._redis_con.get(f"MOTION_VERSION:{self._instance_name}")
                if version is None:
                    raise ValueError("Version not found in Redis.")
                self.version = int(version)

        else:
            if force_update:
                self._loadState()
            self._state.update(new_state)

            # Save state to redis
            saveState(
                self._state,
                self._redis_con,
                self._instance_name,
                self._save_state_func,
            )

            version = self._redis_con.get(f"MOTION_VERSION:{self._instance_name}")
            if version is None:
                raise ValueError("Version not found in Redis.")
            self.version = int(version)

    def _enqueue_and_trigger_update(
        self,
        key: str,
        props: Properties,
        flush_update: bool,
        route_hit: bool,
    ) -> bool:
        # Run the update routes
        # Enqueue results into update queues
        if key in self._update_routes.keys():
            route_hit = True

            # update_events = UpdateEventGroup(key)
            for update_udf_name in self._update_routes[key].keys():
                # If flushing update, just run the route
                if flush_update:
                    route = self._update_routes[key][update_udf_name]

                    # Hold lock

                    with self._redis_con.lock(
                        f"MOTION_LOCK:{self._instance_name}", timeout=120
                    ):
                        try:
                            self._loadState()

                            state_update = route.run(
                                state=self._state,
                                props=props,
                            )

                            if not isinstance(state_update, dict):
                                raise ValueError("State update must be a dict.")
                            else:
                                # Update state
                                self._updateState(
                                    state_update,
                                    force_update=False,
                                    use_lock=False,
                                )
                        except Exception as e:
                            raise RuntimeError(
                                "Error running update route in main process: " + str(e)
                            )

                else:
                    # Enqueue update

                    if self.disable_update_task:
                        raise RuntimeError(
                            f"Update process is disabled. Cannot run update for {key}."
                        )

                    queue_identifier: str = self._get_queue_identifier(
                        key, update_udf_name
                    )

                    identifier = str(uuid4())

                    # Add to update queue
                    self._redis_con.rpush(
                        queue_identifier,
                        cloudpickle.dumps(
                            {
                                "props": props,
                                "identifier": identifier,
                            }
                        ),
                    )

        return route_hit

    async def _async_enqueue_and_trigger_update(
        self,
        key: str,
        props: Properties,
        flush_update: bool,
        route_hit: bool,
    ) -> bool:
        # Run the update routes
        # Enqueue results into update queues
        if key in self._update_routes.keys():
            route_hit = True

            # update_events = UpdateEventGroup(key)
            for update_udf_name in self._update_routes[key].keys():
                # If flushing update, just run the route
                if flush_update:
                    route = self._update_routes[key][update_udf_name]

                    with self._redis_con.lock(
                        f"MOTION_LOCK:{self._instance_name}", timeout=120
                    ):
                        try:
                            self._loadState()

                            state_update = route.run(
                                state=self._state,
                                props=props,
                            )

                            if asyncio.iscoroutine(state_update):
                                state_update = await state_update

                            if not isinstance(state_update, dict):
                                raise ValueError("State update must be a dict.")
                            else:
                                # Update state
                                self._updateState(
                                    state_update,
                                    force_update=False,
                                    use_lock=False,
                                )
                        except Exception as e:
                            raise RuntimeError(
                                "Error running update route in main process: " + str(e)
                            )

                else:
                    # Enqueue update

                    if self.disable_update_task:
                        raise RuntimeError(
                            f"Update process is disabled. Cannot run update for {key}."
                        )

                    queue_identifier: str = self._get_queue_identifier(
                        key, update_udf_name
                    )

                    identifier = str(uuid4())

                    # Add to update queue
                    self._redis_con.rpush(
                        queue_identifier,
                        cloudpickle.dumps(
                            {
                                "props": props,
                                "identifier": identifier,
                            }
                        ),
                    )

        return route_hit

    def _try_cached_serve(
        self,
        key: str,
        props: Properties,
        ignore_cache: bool,
        force_refresh: bool,
    ) -> Tuple[bool, Optional[Any], Properties, Optional[str]]:
        route_run = False
        serve_result = None

        if force_refresh:
            self._loadState()

        # If caching is disabled, return
        if self._cache_ttl == 0:
            return route_run, serve_result, props, None

        # Try hashing the value
        try:
            value_hash = hash_object(props)
        except TypeError:
            value_hash = None

        # Check if key is in cache if value can be hashed and
        # user doesn't want to force refresh state
        if value_hash and not force_refresh and not ignore_cache:
            cache_result_key = f"MOTION_RESULT:{self._instance_name}/{key}/{value_hash}"
            if self._redis_con.exists(cache_result_key):
                new_props = cloudpickle.loads(self._redis_con.get(cache_result_key))
                if new_props._serve_result is not None:
                    props = new_props
                    serve_result = props.serve_result
                    route_run = True

        return route_run, serve_result, props, value_hash

    def run(
        self,
        key: str,
        props: Dict[str, Any],
        ignore_cache: bool,
        force_refresh: bool,
        flush_update: bool,
    ) -> Any:
        route_hit = False
        serve_result = None
        props = Properties(props)

        # Run the serve route
        if key in self._serve_routes.keys():
            route_hit = True
            (
                route_run,
                serve_result,
                props,
                value_hash,
            ) = self._try_cached_serve(key, props, ignore_cache, force_refresh)

            # If not in cache or value can't be hashed or
            # user wants to force refresh state, run route
            if not route_run:
                serve_result = self._serve_routes[key].run(
                    state=self._state, props=props
                )
                props._serve_result = serve_result

                # Check that serve_result is not an awaitable
                if asyncio.iscoroutine(serve_result):
                    raise TypeError(
                        f"Route {key} returned an awaitable. "
                        + "Call `await instance.arun(...)` instead."
                    )

                # Cache result
                if value_hash:
                    cache_result_key = (
                        f"MOTION_RESULT:{self._instance_name}/{key}/{value_hash}"
                    )
                    self.tp.submit(self._setRedis, cache_result_key, props)

        # Run the update routes
        # Enqueue results into update queues
        route_hit = self._enqueue_and_trigger_update(
            key, props, flush_update, route_hit
        )

        if not route_hit:
            raise KeyError(
                f"Key {key} not in routes for component {self._instance_name}."
            )

        return serve_result

    async def arun(
        self,
        key: str,
        props: Dict[str, Any],
        ignore_cache: bool,
        force_refresh: bool,
        flush_update: bool,
    ) -> Any:
        route_hit = False
        serve_result = None
        props = Properties(props)

        # Run the serve route
        if key in self._serve_routes.keys():
            route_hit = True
            (
                route_run,
                serve_result,
                props,
                value_hash,
            ) = self._try_cached_serve(key, props, ignore_cache, force_refresh)

            # If not in cache or value can't be hashed or
            # user wants to force refresh state, run route
            if not route_run:
                serve_result = self._serve_routes[key].run(
                    state=self._state, props=props
                )
                if asyncio.iscoroutine(serve_result):
                    serve_result = await serve_result

                props._serve_result = serve_result

                # Cache result
                if value_hash:
                    cache_result_key = (
                        f"MOTION_RESULT:{self._instance_name}/{key}/{value_hash}"
                    )
                    self.tp.submit(self._setRedis, cache_result_key, props)

        # Run the update routes
        # Enqueue results into update queues
        route_hit = await self._async_enqueue_and_trigger_update(
            key, props, flush_update, route_hit
        )

        if not route_hit:
            raise KeyError(
                f"Key {key} not in routes for component {self._instance_name}."
            )

        return serve_result

    def flush_update(self, dataflow_key: str) -> None:
        # Check if key has update ops
        if dataflow_key not in self._update_routes.keys():
            return

        # Push a noop into the relevant queues
        update_events = UpdateEventGroup(dataflow_key)
        for update_udf_name in self._update_routes[dataflow_key].keys():
            queue_identifier: str = self._get_queue_identifier(
                dataflow_key, update_udf_name
            )
            channel_identifier: str = self._get_channel_identifier(
                dataflow_key, update_udf_name
            )

            identifier = "NOOP_" + str(uuid4())

            # Add pubsub channel to listen to
            update_event = UpdateEvent(self._redis_con, channel_identifier, identifier)
            update_events.add(update_udf_name, update_event)

            # Add to update queue
            self._redis_con.rpush(
                queue_identifier,
                cloudpickle.dumps(
                    {
                        "value": None,
                        "serve_result": None,
                        "identifier": identifier,
                    }
                ),
            )

        # Wait for update result to finish
        update_events.wait()
        # Update state
        self._loadState()
