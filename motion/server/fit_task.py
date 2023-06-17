import asyncio
import multiprocessing
import traceback
from typing import Any, Callable, List, Optional

import cloudpickle
import redis
from redis.lock import Lock

from motion.route import Route
from motion.utils import loadState, logger, saveState


class FitTask(multiprocessing.Process):
    def __init__(
        self,
        instance_name: str,
        route: Route,
        batch_size: int,
        save_state_func: Optional[Callable],
        load_state_func: Optional[Callable],
        queue_identifier: str,
        channel_identifier: str,
        redis_host: str,
        redis_port: int,
        redis_db: int,
        redis_password: str,
        running: Any,
    ):
        super().__init__()
        self.instance_name = instance_name
        self.save_state_func = save_state_func
        self.load_state_func = load_state_func
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_password = redis_password

        self.route = route
        self.batch_size = batch_size
        self.queue_identifier = queue_identifier
        self.channel_identifier = channel_identifier

        # Keep track of batch
        self.batch: List[Any] = []

        # Register the stop event
        # self.running = True
        # self.stop_event = stop_event
        self.running = running
        self.daemon = True

    # def handle_signal(self, signum: int, frame: Any) -> None:
    #     logger.info("Received shutdown signal.")
    #     self.running = False

    def run(self) -> None:
        redis_con = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            password=self.redis_password,
            db=self.redis_db,
        )
        # Acquire a lock
        lock_timeout = 300  # Lock timeout in seconds
        lock = Lock(redis_con, self.instance_name, lock_timeout)

        while self.running.value:
            try:
                for _ in range(self.batch_size):
                    item = redis_con.blpop(self.queue_identifier, timeout=1)
                    if item is None:
                        if not self.running.value:
                            break  # no more items in the list
                        else:
                            continue

                    item, flush_fit = cloudpickle.loads(item[1])
                    self.batch.append(item)
                    if flush_fit:
                        break
            except redis.exceptions.ConnectionError:
                logger.error("Connection to redis lost.")
                break

            # Check if we should stop
            if not self.running.value:
                self.cleanup()
                break

            if not self.batch:
                continue

            # Remove from batch if it was a noop
            values = []
            infer_results = []
            identifiers = []
            for job in self.batch:
                if not job["identifier"].startswith("NOOP_"):
                    values.append(job["value"])
                    infer_results.append(job["infer_result"])
                identifiers.append(job["identifier"])

            # Check that there are elements in values and infer_results
            # Acquire lock and run op
            exception_str = ""
            if len(values) >= 1:
                acquired_lock = lock.acquire(blocking=True)
                if acquired_lock:
                    try:
                        old_state = loadState(
                            redis_con, self.instance_name, self.load_state_func
                        )
                        state_update = self.route.run(
                            state=old_state,
                            values=values,
                            infer_results=infer_results,
                        )
                        # Await if state_update is a coroutine
                        if asyncio.iscoroutine(state_update):
                            state_update = asyncio.run(state_update)

                        if not isinstance(state_update, dict):
                            logger.error(
                                "fit methods should return a dict of state updates."
                            )
                        else:
                            old_state.update(state_update)
                            saveState(
                                old_state,
                                redis_con,
                                self.instance_name,
                                self.save_state_func,
                            )
                    except Exception as e:
                        # logger.error(f"Error in {self.queue_identifier} fit: {e}")
                        logger.error(traceback.format_exc())
                        exception_str = str(e)
                    finally:
                        logger.info("Releasing lock.")
                        lock.release()
                else:
                    logger.error("Lock not acquired; batch lost.")

            for identifier in identifiers:
                redis_con.publish(
                    self.channel_identifier,
                    str({"identifier": identifier, "exception": exception_str}),
                )

            # Clear batch
            self.batch = []

    def cleanup(self) -> None:
        redis_con = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            password=self.redis_password,
            db=self.redis_db,
        )

        # Add outstanding batch back to queue
        for item in self.batch:
            # Pickle item object
            pickled_item = cloudpickle.dumps(
                (
                    item,
                    False,  # flush_fit should be False
                )
            )

            redis_con.lpush(self.queue_identifier, pickled_item)

        self.batch = []
