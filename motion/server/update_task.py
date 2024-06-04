import asyncio
import time
import traceback
from multiprocessing import Process
from threading import Thread
from typing import Any, Callable, Dict, List, Optional

import cloudpickle
import redis
import requests

from motion.route import Route
from motion.utils import FlowOpStatus, loadState, logger, saveState


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
        victoria_metrics_url: Optional[str] = None,
    ):
        super().__init__()
        self.task_type = task_type
        self.name = f"UpdateTask-{task_type}-{instance_name}"
        self.instance_name = instance_name

        self._component_name, self._instance_id = instance_name.split("__")

        self.victoria_metrics_url = victoria_metrics_url

        self.save_state_func = save_state_func
        self.load_state_func = load_state_func

        self.routes = routes
        self.queue_identifiers = queue_identifiers
        self.channel_identifiers = channel_identifiers
        self.lock_identifier = lock_identifier

        self.running = running
        self.daemon = True

        self.redis_params = redis_params

    def _logMessage(
        self,
        flow_key: str,
        op_type: str,
        status: FlowOpStatus,
        duration: float,
        func_name: str,
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

    def custom_run(self) -> None:
        try:
            redis_con = None
            while self.running.value:
                if not redis_con:
                    redis_con = redis.Redis(**self.redis_params)

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
                            raise ValueError(
                                f"State for {self.instance_name} not found."
                            )

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
                if self.victoria_metrics_url:
                    try:
                        flow_key = queue_name.split("/")[-2]
                        udf_name = queue_name.split("/")[-1]
                        self._logMessage(
                            flow_key,
                            "update",
                            (
                                FlowOpStatus.SUCCESS
                                if not exception_str
                                else FlowOpStatus.FAILURE
                            ),
                            duration,
                            udf_name,
                        )
                    except Exception as e:
                        logger.error(
                            f"Error logging to VictoriaMetrics: {e}", exc_info=True
                        )

        finally:
            if redis_con:
                redis_con.close()


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
