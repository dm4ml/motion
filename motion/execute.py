import asyncio
import multiprocessing
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple
from uuid import uuid4

import cloudpickle
import psutil
import redis
from redis.lock import Lock

from motion.dicts import Properties, State
from motion.route import Route
from motion.server.update_task import UpdateTask
from motion.utils import (
    RedisParams,
    UpdateEvent,
    UpdateEventGroup,
    hash_object,
    loadState,
    logger,
    saveState,
)


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
        disabled: bool = False,
    ):
        self._instance_name = instance_name
        self._cache_ttl = cache_ttl

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
            self._state = self._loadState()
            self.version = self._redis_con.get(f"MOTION_VERSION:{self._instance_name}")

        self.version = int(self.version)

        # Set up routes
        self._serve_routes: Dict[str, Route] = serve_routes
        self._update_routes: Dict[str, Dict[str, Route]] = {
            rkey: {route.udf.__name__: route for route in routes}
            for rkey, routes in update_routes.items()
        }

        # Set up shutdown event
        # self._shutdown_event = threading.Event()

        # Set up update queues, batch sizes, and threads
        self.disabled = disabled
        if not disabled:
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

    def _loadState(self) -> State:
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
        """Builds update job."""
        rp = RedisParams()
        # self.worker_states = {}

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
            self.worker_task = UpdateTask(
                self._instance_name,
                routes=self.route_dict_for_fit,
                save_state_func=self._save_state_func,
                load_state_func=self._load_state_func,
                queue_identifiers=self.queue_ids_for_fit,
                channel_identifiers=self.channel_dict_for_fit,
                redis_host=rp.host,
                redis_port=rp.port,
                redis_db=rp.db,
                redis_password=rp.password,  # type: ignore
                running=self.running,
            )
            self.worker_task.start()

        # Set up a monitor thread
        self.stop_event = threading.Event()
        self.monitor_thread = threading.Thread(
            target=self._monitor_process, daemon=True
        )
        self.monitor_thread.start()

    def _monitor_process(self) -> None:
        if not self.worker_task:
            return

        rp = RedisParams()
        while not self.stop_event.is_set():
            # See if the update task is alive
            if not self.worker_task.is_alive():
                logger.debug(
                    f"Failed to detect heartbeat for {self.worker_task.name}."
                    + " Restarting the task in the background."
                )

                # Restart
                self.worker_task = UpdateTask(
                    self._instance_name,
                    routes=self.route_dict_for_fit,
                    save_state_func=self._save_state_func,
                    load_state_func=self._load_state_func,
                    queue_identifiers=self.queue_ids_for_fit,
                    channel_identifiers=self.channel_dict_for_fit,
                    redis_host=rp.host,
                    redis_port=rp.port,
                    redis_db=rp.db,
                    redis_password=rp.password,  # type: ignore
                    running=self.running,
                )
                self.worker_task.start()

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
        if self.disabled:
            return

        if not self.running.value:
            return

        if is_open:
            logger.info("Running update operations on remaining data...")

        # Set shutdown event
        self.stop_event.set()
        self.running.value = False

        if self.worker_task and psutil.pid_exists(self.worker_task.pid):
            self.worker_task.join()

        self._redis_con.close()

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

            update_events = UpdateEventGroup(key)
            for update_udf_name in self._update_routes[key].keys():
                queue_identifier: str = self._get_queue_identifier(key, update_udf_name)
                channel_identifier: str = self._get_channel_identifier(
                    key, update_udf_name
                )

                identifier = str(uuid4())

                if flush_update:
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
                            "props": props,
                            "identifier": identifier,
                        }
                    ),
                )

            if flush_update:
                # Wait for update result to finish
                update_events.wait()
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
            self._state = self._loadState()
            v = self._redis_con.get(f"MOTION_VERSION:{self._instance_name}")
            if not v:
                raise ValueError(
                    f"Error loading state for {self._instance_name}."
                    + " No version found."
                )
            self.version = int(v)

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
                props = cloudpickle.loads(self._redis_con.get(cache_result_key))
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
                    self._redis_con.set(
                        cache_result_key,
                        cloudpickle.dumps(props),
                        ex=self._cache_ttl,
                    )

        # Run the update routes
        # Enqueue results into update queues
        route_hit = self._enqueue_and_trigger_update(
            key, props, flush_update, route_hit
        )

        if not route_hit:
            raise KeyError(f"Key {key} not in routes.")

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
                serve_result_awaitable = self._serve_routes[key].run(
                    state=self._state, props=props
                )
                if not asyncio.iscoroutine(serve_result_awaitable):
                    raise TypeError(
                        f"Route {key} returned a non-awaitable. "
                        + "Call `instance.run(...)` instead."
                    )

                serve_result = await serve_result_awaitable
                props._serve_result = serve_result

                # Cache result
                if value_hash:
                    cache_result_key = (
                        f"MOTION_RESULT:{self._instance_name}/{key}/{value_hash}"
                    )
                    self._redis_con.set(
                        cache_result_key,
                        cloudpickle.dumps(props),
                        ex=self._cache_ttl,
                    )

        # Run the update routes
        # Enqueue results into update queues
        route_hit = self._enqueue_and_trigger_update(
            key, props, flush_update, route_hit
        )

        if not route_hit:
            raise KeyError(f"Key {key} not in routes.")

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
        self._state = self._loadState()
        v = self._redis_con.get(f"MOTION_VERSION:{self._instance_name}")
        if not v:
            raise ValueError(
                f"Error loading state for {self._instance_name}." + " No version found."
            )

        self.version = int(v)
