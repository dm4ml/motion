import os
import signal
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

import cloudpickle
import redis

from motion.fit_task import FitTask
from motion.route import Route
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

        self.running = False
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
        self.running = True

        # Set up state
        self.version = self._redis_con.get(f"MOTION_VERSION:{self._instance_name}")
        self._state = CustomDict(self._instance_name, "state", {})
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
        rp = RedisParams()
        # self.worker_states = {}

        for rkey, routes in self._fit_routes.items():
            for udf_name, route in routes.items():
                pname = f"{self._instance_name}_{rkey}_{udf_name}_fit"
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
                )
                self.worker_tasks[pname].start()

    def _get_queue_identifier(self, route_key: str, udf_name: str) -> str:
        """Gets the queue identifier for a given route key and UDF name."""
        return f"MOTION_QUEUE:{self._instance_name}/{route_key}/{udf_name}"

    def _get_channel_identifier(self, route_key: str, udf_name: str) -> str:
        """Gets the channel identifier for a given route key and UDF name."""
        return f"MOTION_CHANNEL:{self._instance_name}/{route_key}/{udf_name}"

    def shutdown(self, is_open: bool) -> None:
        if not self.running:
            return

        if is_open:
            logger.info("Running fit operations on remaining data...")

        # Set shutdown event
        for process in self.worker_tasks.values():
            os.kill(process.pid, signal.SIGUSR1)  # type:ignore

        self._redis_con.close()

        # Join fit threads
        for process in self.worker_tasks.values():
            # if self.worker_states.get(thread.name, False):
            process.join()

    def update(self, new_state: Dict[str, Any]) -> None:
        if new_state:
            self._state.update(new_state)
            # Save state to redis
            saveState(
                self._state,
                self._redis_con,
                self._instance_name,
                self._save_state_func,
            )

    def empty_batch(self) -> Dict[str, List[Any]]:
        return {
            "fit_events": [],
            "values": [],
            "infer_results": [],
        }

    def run(
        self,
        key: str,
        value: Any,
        cache_ttl: int,
        force_refresh: bool,
        force_fit: bool,
    ) -> Any:
        route_hit = False
        infer_result = None

        # Run the infer route
        if key in self._infer_routes.keys():
            route_hit = True
            route_run = False

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
            if value_hash and not force_refresh:
                cache_result_key = (
                    f"MOTION_RESULT:{self._instance_name}/{key}/{value_hash}"
                )
                if self._redis_con.exists(cache_result_key):
                    infer_result = cloudpickle.loads(
                        self._redis_con.get(cache_result_key)
                    )
                    # Update TTL
                    self._redis_con.expire(cache_result_key, cache_ttl)
                    route_run = True

            # If not in cache or value can't be hashed or
            # user wants to force refresh state, run route
            if not route_run:
                infer_result = self._infer_routes[key].run(
                    state=self._state, value=value
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
        if key in self._fit_routes.keys():
            route_hit = True

            fit_events = FitEventGroup(key)
            for fit_udf_name in self._fit_routes[key].keys():
                queue_identifier: str = self._get_queue_identifier(key, fit_udf_name)
                channel_identifier: str = self._get_channel_identifier(
                    key, fit_udf_name
                )

                identifier = str(uuid4())

                if force_fit:
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
                            force_fit,
                        )
                    ),
                )

            if force_fit:
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

        if not route_hit:
            raise KeyError(f"Key {key} not in routes.")

        return infer_result
