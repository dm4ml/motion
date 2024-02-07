import asyncio
import os
import time
import traceback
from multiprocessing import Process
from threading import Thread
from typing import Any, Callable, Dict, List, Optional

import cloudpickle
import redis

from motion.dashboard_utils import create_prometheus_metrics
from motion.route import Route
from motion.utils import FlowOpStatus, loadState, logger, saveState

if os.getenv("MOTION_VICTORIAMETRICS_URL"):
    from prometheus_client import CollectorRegistry, push_to_gateway


class BaseUpdateTask:
    def __init__(
        self,
        task_type: str,
        instance_name: str,
        routes: Dict[str, Route],
        save_state_func: Optional[Callable],
        load_state_func: Optional[Callable],
        queue_identifiers: List[str],
        channel_identifiers: Dict[str, str],
        lock_identifier: str,
        redis_params: Dict[str, Any],
        running: Any,
        victoriametrics_url: Optional[str] = None,
    ):
        super().__init__()
        self.task_type = task_type
        self.name = f"UpdateTask-{task_type}-{instance_name}"
        self.instance_name = instance_name

        self._component_name, self._instance_id = instance_name.split("__")
        if victoriametrics_url is not None:
            (
                self.op_duration,
                self.op_success,
                self.op_failure,
            ) = create_prometheus_metrics(victoriametrics_url)

        self.victoriametrics_url = victoriametrics_url

        self.registry = CollectorRegistry() if victoriametrics_url else None

        self.save_state_func = save_state_func
        self.load_state_func = load_state_func

        self.routes = routes
        self.queue_identifiers = queue_identifiers
        self.channel_identifiers = channel_identifiers
        self.lock_identifier = lock_identifier

        self.running = running
        self.daemon = True

        self.redis_params = redis_params
        self.redis_con: Optional[redis.Redis] = None

    def _logMessage(
        self,
        flow_key: str,
        status: FlowOpStatus,
        duration: float,
        func_name: str = "",
    ) -> None:
        """Method to log a message."""

        # Log to VictoriaMetrics
        if status == FlowOpStatus.SUCCESS:
            self.op_success.labels(
                component_name=self._component_name,
                instance_id=self._instance_id,
                op_name=f"{flow_key}/{func_name}",
                op_type="update",
            ).inc()

        else:
            self.op_failure.labels(
                component_name=self._component_name,
                instance_id=self._instance_id,
                op_name=f"{flow_key}/{func_name}",
                op_type="update",
            ).inc()

        # Log duration
        self.op_duration.labels(
            component_name=self._component_name,
            instance_id=self._instance_id,
            op_name=f"{flow_key}/{func_name}",
            op_type="update",
        ).set(duration)

        # Push to gateway
        push_to_gateway(self.victoriametrics_url, job=self.name, registry=self.registry)

    def __del__(self) -> None:
        if self.redis_con is not None:
            self.redis_con.close()

    def custom_run(self) -> None:
        self.redis_con = redis.Redis(**self.redis_params)
        redis_con = self.redis_con

        while self.running.value:
            item: Dict[str, Any] = {}
            queue_name = ""
            try:
                # for _ in range(self.batch_size):
                full_item = redis_con.blpop(self.queue_identifiers, timeout=0.01)
                if full_item is None:
                    if not self.running.value:
                        break  # no more items in the list
                    else:
                        continue

                queue_name = full_item[0].decode("utf-8")
                item = cloudpickle.loads(full_item[1])
                # self.batch.append(item)
                # if flush_update:
                #     break
            except redis.exceptions.ConnectionError:
                logger.error("Connection to redis lost.", exc_info=True)
                break

            # Check if we should stop
            if not self.running.value and not item:
                # self.cleanup()
                break

            # if not self.batch:
            #     continue

            exception_str = ""
            # Check if it was a no op
            if item["identifier"].startswith("NOOP_"):
                redis_con.publish(
                    self.channel_identifiers[queue_name],
                    str(
                        {
                            "identifier": item["identifier"],
                            "exception": exception_str,
                        }
                    ),
                )
                continue

            # Check if item.get("expire_at") has passed
            expire_at = item.get("expire_at")
            if expire_at is not None:
                if expire_at < redis_con.time()[0]:
                    redis_con.publish(
                        self.channel_identifiers[queue_name],
                        str(
                            {
                                "identifier": item["identifier"],
                                "exception": "Expired",
                            }
                        ),
                    )
                    continue

            # Run update op
            try:
                start_time = time.time()
                with redis_con.lock(self.lock_identifier, timeout=120):
                    old_state, version = loadState(
                        redis_con,
                        self.instance_name,
                        self.load_state_func,
                    )
                    if old_state is None:
                        # Create new state
                        # If state does not exist, run setUp
                        raise ValueError(f"State for {self.instance_name} not found.")

                    state_update = self.routes[queue_name].run(
                        state=old_state,
                        props=item["props"],
                    )
                    # Await if state_update is a coroutine
                    if asyncio.iscoroutine(state_update):
                        state_update = asyncio.run(state_update)

                    if not isinstance(state_update, dict):
                        logger.error(
                            "Update methods should return a dict of state updates.",
                            exc_info=True,
                        )
                    else:
                        old_state.update(state_update)
                        saveState(
                            old_state,
                            version,
                            redis_con,
                            self.instance_name,
                            self.save_state_func,
                        )

            except Exception:
                logger.error(traceback.format_exc())
                exception_str = str(traceback.format_exc())

            duration = time.time() - start_time

            redis_con.publish(
                self.channel_identifiers[queue_name],
                str(
                    {
                        "identifier": item["identifier"],
                        "exception": exception_str,
                    }
                ),
            )

            # Log to VictoriaMetrics
            if self.victoriametrics_url:
                try:
                    flow_key = queue_name.split("/")[-2]
                    udf_name = queue_name.split("/")[-1]
                    self._logMessage(
                        flow_key,
                        FlowOpStatus.SUCCESS
                        if not exception_str
                        else FlowOpStatus.FAILURE,
                        duration,
                        udf_name,
                    )
                except Exception as e:
                    logger.error(
                        f"Error logging to VictoriaMetrics: {e}", exc_info=True
                    )


class UpdateProcess(Process):
    def __init__(
        self,
        **kwargs: Any,
    ) -> None:
        super().__init__()
        self.name = f"UpdateTask-{kwargs.get('instance_name', '')}"
        self.daemon = True
        self.but = BaseUpdateTask(task_type="process", **kwargs)

    def run(self) -> None:
        self.but.custom_run()


class UpdateThread(Thread):
    def __init__(
        self,
        **kwargs: Any,
    ) -> None:
        super().__init__()
        self.name = f"UpdateTask-{kwargs.get('instance_name', '')}"
        self.daemon = True
        self.but = BaseUpdateTask(task_type="thread", **kwargs)

    def run(self) -> None:
        self.but.custom_run()
