import asyncio
import inspect
import logging
import multiprocessing
import os
import threading
import time
import types
from concurrent.futures import ThreadPoolExecutor
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    Generator,
    List,
    Literal,
    Optional,
    Tuple,
)
from uuid import uuid4

import cloudpickle
import psutil
import redis
import requests

from motion.dicts import Properties, State
from motion.discard_policy import DiscardPolicy
from motion.route import Route
from motion.server.update_task import UpdateProcess, UpdateThread
from motion.utils import (
    FlowOpStatus,
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
        self._component_name = instance_name.split("__")[0]
        self._instance_id = instance_name.split("__")[1]
        self._cache_ttl = cache_ttl
        self._num_messages = 100

        # VictoriaMetrics Configuration
        self.victoria_metrics_url = os.getenv("MOTION_VICTORIAMETRICS_URL")

        self._init_state_func = init_state_func
        self._init_state_params = init_state_params
        self._load_state_func = load_state_func
        self._save_state_func = save_state_func
        self.__lock_prefix = (
            f"MOTION_LOCK:DEV:{self._instance_name}"
            if os.getenv("MOTION_ENV", "prod") == "dev"
            else f"MOTION_LOCK:{self._instance_name}"
        )
        self.__queue_prefix = (
            f"MOTION_QUEUE:DEV:{self._instance_name}"
            if os.getenv("MOTION_ENV", "prod") == "dev"
            else f"MOTION_QUEUE:{self._instance_name}"
        )
        self.__channel_prefix = (
            f"MOTION_CHANNEL:DEV:{self._instance_name}"
            if os.getenv("MOTION_ENV", "prod") == "dev"
            else f"MOTION_CHANNEL:{self._instance_name}"
        )
        self.__cache_result_prefix = (
            f"MOTION_RESULT:DEV:{self._instance_name}"
            if os.getenv("MOTION_ENV", "prod") == "dev"
            else f"MOTION_RESULT:{self._instance_name}"
        )

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

        # If version does not exist, load state
        self.version: Optional[int] = None
        self._loadState(only_create=True)

        # Set up routes
        self._serve_routes: Dict[str, Route] = serve_routes
        self._update_routes: Dict[str, Dict[str, Route]] = {
            rkey: {route.udf.__name__: route for route in routes}
            for rkey, routes in update_routes.items()
        }

        # Set up update queues, batch sizes, and threads
        self.disable_update_task = disable_update_task
        if not disable_update_task:
            self.update_task_type = update_task_type
            self._build_fit_jobs()

        self.tp = ThreadPoolExecutor(max_workers=2)

        # Add component name to set of components if we are not in dev mode
        if os.getenv("MOTION_ENV", "prod") != "dev":
            self._redis_con.sadd("MOTION_COMPONENTS", self._component_name)

    def _setRedis(self, cache_result_key: str, props: Any) -> None:
        """Method to set value in Redis."""
        self._redis_con.set(
            cache_result_key, cloudpickle.dumps(props), ex=self._cache_ttl
        )

    def _logMessage(
        self,
        flow_key: str,
        op_type: str,
        status: FlowOpStatus,
        duration: float,
        func_name: str = "",
    ) -> None:
        """Method to log a message directly to VictoriaMetrics using InfluxDB
        line protocol."""
        if self.victoria_metrics_url:
            timestamp = int(
                time.time() * 1000000000
            )  # Nanoseconds for InfluxDB line protocol
            status_label = "success" if status == FlowOpStatus.SUCCESS else "failure"

            # Format the metric in InfluxDB line protocol
            metric_data = f"motion_operation_duration_seconds,component={self._component_name},instance={self._instance_id},flow={flow_key},op_type={op_type},status={status_label} value={duration} {timestamp}"  # noqa: E501

            # Format the counter metrics for success and failure
            success_counter = f"motion_operation_success_count,component={self._component_name},instance={self._instance_id},flow={flow_key},op_type={op_type} value={1 if status_label == 'success' else 0} {timestamp}"  # noqa: E501
            failure_counter = f"motion_operation_failure_count,component={self._component_name},instance={self._instance_id},flow={flow_key},op_type={op_type} value={1 if status_label == 'failure' else 0} {timestamp}"  # noqa: E501

            # Combine metrics into a single payload with newline character
            payload = "\n".join([metric_data, success_counter, failure_counter])

            try:
                # Send HTTP POST request with the combined metric data
                response = requests.post(
                    self.victoria_metrics_url + "/write", data=payload
                )
                response.raise_for_status()  # Raise an exception for HTTP errors
            except requests.RequestException as e:
                logger.error(f"Failed to send metric to VictoriaMetrics: {e}")

    def _connectToRedis(self) -> Tuple[RedisParams, redis.Redis]:
        rp = get_redis_params()

        # Put a timeout on the connection
        param_dict = rp.dict()
        if "socket_timeout" not in param_dict:
            param_dict["socket_timeout"] = self._redis_socket_timeout

        # Pop all None values
        param_dict = {k: v for k, v in param_dict.items() if v is not None}

        r = redis.Redis(**param_dict)
        return rp, r

    def _loadVersion(self) -> Optional[int]:
        # If in dev mode, try loading dev
        redis_v = None
        if os.getenv("MOTION_ENV", "prod") == "dev":
            redis_v = self._redis_con.get(f"MOTION_VERSION:DEV:{self._instance_name}")

        if not redis_v:
            redis_v = self._redis_con.get(f"MOTION_VERSION:{self._instance_name}")

        return int(redis_v) if redis_v else None

    def _loadState(self, only_create: bool = False) -> None:
        # If in dev mode, try loading dev state
        redis_v = self._loadVersion()
        if not redis_v:
            # If state does not exist, run setUp
            with self._redis_con.lock(self.__queue_prefix, timeout=120):
                # If state was created while waiting for lock, don't do
                # anything
                redis_v = self._loadVersion()
                loaded_state = False

                if not redis_v:
                    state = State(
                        self._instance_name.split("__")[0],
                        self._instance_name.split("__")[1],
                        {},
                    )
                    state.update(self.setUp(**self._init_state_params))
                    version = saveState(
                        state,
                        0,
                        self._redis_con,
                        self._instance_name,
                        self._save_state_func,
                    )
                    assert version == 1, "Version should be 1 after saving state."
                    loaded_state = True

            if loaded_state:
                self._state = state
                self.version = version
                return

        if not only_create:
            if self.version is None or (self.version and self.version < redis_v):  # type: ignore # noqa: E501
                # Reload state
                new_state, self.version = loadState(
                    self._redis_con, self._instance_name, self._load_state_func
                )
                if new_state is None:
                    raise ValueError(
                        f"Error loading state for {self._instance_name}."
                        + " State is None."
                    )
                self._state = new_state

    def _saveState(self, new_state: State) -> None:
        assert self.version is not None, "Version should not be None."

        # Save state to redis
        new_version = saveState(
            new_state,
            self.version,
            self._redis_con,
            self._instance_name,
            self._save_state_func,
        )
        if new_version == -1:
            logger.error(
                f"Error saving state to Redis for {self._instance_name}:"
                + " there was a newer state found."
            )
            # Reload state
            self._loadState()
        else:
            self.version = new_version

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
                lock_identifier=self.__lock_prefix,
                redis_params=self._redis_params.dict(),
                running=self.running,
                victoria_metrics_url=self.victoria_metrics_url,
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
                    lock_identifier=self.__lock_prefix,
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
        return f"{self.__queue_prefix}/{route_key}/{udf_name}"

    def _get_channel_identifier(self, route_key: str, udf_name: str) -> str:
        """Gets the channel identifier for a given route key and UDF name."""
        return f"{self.__channel_prefix}/{route_key}/{udf_name}"

    def shutdown(self, is_open: bool, wait_for_logging_threads: bool) -> None:
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

        # Shut down threadpool for writing to Redis and logging
        self.tp.shutdown(wait=wait_for_logging_threads)

        self._redis_con.close()

        self.monitor_thread.join()

        # Delete self.running
        self.running = None
        del self.running

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
            with self._redis_con.lock(self.__lock_prefix, timeout=120):
                if force_update:
                    self._loadState()
                self._state.update(new_state)

                # Save state to redis
                self._saveState(self._state)

        else:
            if force_update:
                self._loadState()
            self._state.update(new_state)

            # Save state to redis
            self._saveState(self._state)

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
                    start_time = time.time()

                    with self._redis_con.lock(self.__lock_prefix, timeout=120):
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

                            # Log message
                            if self.victoria_metrics_url:
                                self.tp.submit(
                                    self._logMessage,
                                    key,
                                    "update",
                                    FlowOpStatus.SUCCESS,
                                    time.time() - start_time,
                                    route.udf.__name__,
                                )

                        except Exception as e:
                            # Log message
                            if self.victoria_metrics_url:
                                self.tp.submit(
                                    self._logMessage,
                                    key,
                                    "update",
                                    FlowOpStatus.FAILURE,
                                    time.time() - start_time,
                                    route.udf.__name__,
                                )

                            raise RuntimeError(
                                "Error running update route in main process: " + str(e)
                            )

                else:
                    # Enqueue update

                    if self.disable_update_task:
                        raise RuntimeError(
                            f"Update process is disabled. Cannot run update for {key}."
                        )

                    func = self._update_routes[key][update_udf_name].udf

                    queue_identifier: str = self._get_queue_identifier(
                        key, update_udf_name
                    )

                    identifier = str(uuid4())

                    # If the func has a discard_after attribute, expire_at
                    # = current time + discard_after
                    expire_at = (
                        self._redis_con.time()[0] + func._discard_after  # type: ignore
                        if func._discard_policy == DiscardPolicy.SECONDS  # type: ignore
                        else None
                    )  # type: ignore

                    # Add to update queue
                    self._redis_con.rpush(
                        queue_identifier,
                        cloudpickle.dumps(
                            {
                                "props": props,
                                "identifier": identifier,
                                "expire_at": expire_at,
                            }
                        ),
                    )

                    # If the func has a discard_after attribute, delete
                    # old items in a queue
                    if func._discard_after is not None:  # type: ignore
                        if func._discard_policy == DiscardPolicy.NUM_NEW_UPDATES:  # type: ignore # noqa: E501
                            # Get the length of the queue
                            queue_length = self._redis_con.llen(queue_identifier)
                            # If the queue length is greater than the
                            # discard_after attribute, delete the oldest
                            # (queue_length - discard_after) items
                            if queue_length > func._discard_after:  # type: ignore
                                self._redis_con.ltrim(
                                    queue_identifier,
                                    queue_length - func._discard_after,  # type: ignore
                                    -1,
                                )

                        elif func._discard_policy == DiscardPolicy.SECONDS:  # type: ignore # noqa: E501
                            # Need to delete items that are older than
                            # discard_after seconds
                            # Can just do this in the update task
                            pass

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

                    start_time = time.time()

                    with self._redis_con.lock(self.__lock_prefix, timeout=120):
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

                            # Log message
                            if self.victoria_metrics_url:
                                self.tp.submit(
                                    self._logMessage,
                                    key,
                                    "update",
                                    FlowOpStatus.SUCCESS,
                                    time.time() - start_time,
                                    route.udf.__name__,
                                )

                        except Exception as e:
                            # Log message
                            if self.victoria_metrics_url:
                                self.tp.submit(
                                    self._logMessage,
                                    key,
                                    "update",
                                    FlowOpStatus.FAILURE,
                                    time.time() - start_time,
                                    route.udf.__name__,
                                )

                            raise RuntimeError(
                                "Error running update route in main process: " + str(e)
                            )

                else:
                    # Enqueue update
                    if self.disable_update_task:
                        raise RuntimeError(
                            f"Update process is disabled. Cannot run update for {key}."
                        )

                    func = self._update_routes[key][update_udf_name].udf
                    queue_identifier: str = self._get_queue_identifier(
                        key, update_udf_name
                    )

                    identifier = str(uuid4())

                    # If the func has a discard_after attribute, expire_at
                    # = current time + discard_after
                    expire_at = (
                        self._redis_con.time()[0] + func._discard_after  # type: ignore
                        if func._discard_policy == DiscardPolicy.SECONDS  # type: ignore
                        else None
                    )

                    # Add to update queue
                    self._redis_con.rpush(
                        queue_identifier,
                        cloudpickle.dumps(
                            {
                                "props": props,
                                "identifier": identifier,
                                "expire_at": expire_at,
                            }
                        ),
                    )

                    # If the func has a discard_after attribute, delete
                    # old items in a queue
                    if func._discard_after is not None:  # type: ignore
                        if func._discard_policy == DiscardPolicy.NUM_NEW_UPDATES:  # type: ignore # noqa: E501
                            # Get the length of the queue
                            queue_length = self._redis_con.llen(queue_identifier)
                            # If the queue length is greater than the
                            # discard_after attribute, delete the oldest
                            # (queue_length - discard_after) items
                            if queue_length > func._discard_after:  # type: ignore
                                self._redis_con.ltrim(
                                    queue_identifier,
                                    queue_length - func._discard_after,  # type: ignore
                                    -1,
                                )

                        elif func._discard_policy == DiscardPolicy.SECONDS:  # type: ignore # noqa: E501
                            # Need to delete items that are older than
                            # discard_after seconds
                            # Can just do this in the update task
                            pass

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
            self.flush_update()

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
            cache_result_key = f"{self.__cache_result_prefix}/{key}/{value_hash}"
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
    ) -> Generator[Any, None, None]:
        try:
            route_hit = False
            serve_result = None
            is_generated = False
            props = Properties(props)

            # Run the serve route
            start_time = time.time()
            if key in self._serve_routes.keys():
                route_hit = True
                (
                    route_run,
                    serve_result,
                    props,
                    value_hash,
                ) = self._try_cached_serve(key, props, ignore_cache, force_refresh)

                # If route is run and serve result is not None and self.
                # _serve_routes[key].udf is a generator, iterate through the serve
                # result
                if route_run and inspect.isgeneratorfunction(
                    self._serve_routes[key].udf
                ):
                    is_generated = True

                    # Process each item yielded by the generator
                    if serve_result is not None:
                        for item in serve_result:
                            yield item

                # If not in cache or value can't be hashed or
                # user wants to force refresh state, run route
                if not route_run:
                    self._loadState()
                    serve_result = self._serve_routes[key].run(
                        state=self._state, props=props
                    )

                    # Check if the serve_result is a generator (streaming result)
                    if isinstance(serve_result, types.GeneratorType):
                        # Accumulate items from generator
                        is_generated = True
                        accumulated_result = []

                        # Process each item yielded by the generator
                        for item in serve_result:
                            accumulated_result.append(item)
                            yield item  # Yield the item for streaming

                        serve_result = accumulated_result

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
                            f"{self.__cache_result_prefix}/{key}/{value_hash}"
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

            duration = time.time() - start_time

            if not is_generated:
                yield serve_result

            if self.victoria_metrics_url:
                self.tp.submit(
                    self._logMessage, key, "serve", FlowOpStatus.SUCCESS, duration
                )

        except Exception as e:
            duration = time.time() - start_time

            if self.victoria_metrics_url and key in self._serve_routes.keys():
                self.tp.submit(
                    self._logMessage, key, "serve", FlowOpStatus.FAILURE, duration
                )

            raise e

    async def arun(
        self,
        key: str,
        props: Dict[str, Any],
        ignore_cache: bool,
        force_refresh: bool,
        flush_update: bool,
    ) -> AsyncGenerator[Any, None]:
        try:
            route_hit = False
            serve_result = None
            props = Properties(props)

            # Run the serve route
            is_generated = False
            start_time = time.time()
            if key in self._serve_routes.keys():
                route_hit = True
                (
                    route_run,
                    serve_result,
                    props,
                    value_hash,
                ) = self._try_cached_serve(key, props, ignore_cache, force_refresh)

                # If route is run and serve result is not None and self.
                # _serve_routes[key].udf is a generator, iterate through the serve
                # result
                if route_run and inspect.isasyncgenfunction(
                    self._serve_routes[key].udf
                ):
                    is_generated = True

                    # Process each item yielded by the generator
                    if serve_result is not None:
                        for item in serve_result:
                            yield item

                # If not in cache or value can't be hashed or
                # user wants to force refresh state, run route
                if not route_run:
                    self._loadState()
                    serve_result = self._serve_routes[key].run(
                        state=self._state, props=props
                    )
                    # Check if the serve_result is an async generator (streaming result)
                    if isinstance(serve_result, types.AsyncGeneratorType):
                        # Accumulate items from generator
                        is_generated = True
                        accumulated_result = []

                        # Process each item yielded by the generator
                        # but don't trigger a "return statement with value is
                        # not allowed in an async generator" error
                        async for item in serve_result:
                            accumulated_result.append(item)
                            yield item

                        serve_result = accumulated_result

                    elif asyncio.iscoroutine(serve_result):
                        serve_result = await serve_result

                    props._serve_result = serve_result

                    # Cache result
                    if value_hash:
                        cache_result_key = (
                            f"{self.__cache_result_prefix}/{key}/{value_hash}"
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

            duration = time.time() - start_time

            if not is_generated:
                yield serve_result

            if self.victoria_metrics_url:
                self.tp.submit(
                    self._logMessage, key, "serve", FlowOpStatus.SUCCESS, duration
                )

        except Exception as e:
            duration = time.time() - start_time
            if self.victoria_metrics_url and key in self._serve_routes.keys():
                self.tp.submit(
                    self._logMessage, key, "serve", FlowOpStatus.FAILURE, duration
                )

            raise e

    def flush_update(self, flow_key: str = "*ALL*") -> None:
        flow_keys: List[str] = []

        # If flow_key is *ALL*, flush all update queues
        if flow_key == "*ALL*":
            flow_keys = list(self._update_routes.keys())

        # Check if key has update ops
        elif flow_key not in self._update_routes.keys():
            return

        else:
            flow_keys = [flow_key]

        # Push a noop into the relevant queues
        for flow_key in flow_keys:
            update_events = UpdateEventGroup(flow_key)
            for update_udf_name in self._update_routes[flow_key].keys():
                queue_identifier: str = self._get_queue_identifier(
                    flow_key, update_udf_name
                )
                channel_identifier: str = self._get_channel_identifier(
                    flow_key, update_udf_name
                )

                identifier = "NOOP_" + str(uuid4())

                # Add pubsub channel to listen to
                update_event = UpdateEvent(
                    self._redis_con, channel_identifier, identifier
                )
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
