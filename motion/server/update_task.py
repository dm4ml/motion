import asyncio
import multiprocessing
import traceback
from typing import Any, Callable, Dict, List, Optional

import cloudpickle
import redis
from redis.lock import Lock

from motion.route import Route
from motion.utils import loadState, logger, saveState


class UpdateTask(multiprocessing.Process):
    def __init__(
        self,
        instance_name: str,
        routes: Dict[str, Route],
        save_state_func: Optional[Callable],
        load_state_func: Optional[Callable],
        queue_identifiers: List[str],
        channel_identifiers: Dict[str, str],
        redis_host: str,
        redis_port: int,
        redis_db: int,
        redis_password: str,
        running: Any,
    ):
        super().__init__()
        self.name = f"UpdateTask-{instance_name}"
        self.instance_name = instance_name
        self.save_state_func = save_state_func
        self.load_state_func = load_state_func
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_password = redis_password

        self.routes = routes
        self.queue_identifiers = queue_identifiers
        self.channel_identifiers = channel_identifiers

        self.running = running
        self.daemon = True

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
            item: Dict[str, Any] = {}
            queue_name = ""
            try:
                # for _ in range(self.batch_size):
                full_item = redis_con.blpop(self.queue_identifiers, timeout=0.1)
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
                logger.error("Connection to redis lost.")
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

            # Run update op
            acquired_lock = lock.acquire(blocking=True)
            if acquired_lock:
                try:
                    old_state = loadState(
                        redis_con,
                        self.instance_name,
                        self.load_state_func,
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
                            "Update methods should return a dict of state updates."
                        )
                    else:
                        old_state.update(state_update)
                        saveState(
                            old_state,
                            redis_con,
                            self.instance_name,
                            self.save_state_func,
                        )
                except Exception:
                    logger.error(traceback.format_exc())
                    exception_str = str(traceback.format_exc())
                finally:
                    logger.info("Releasing lock.")
                    lock.release()
            else:
                logger.error("Lock not acquired; item lost.")

            redis_con.publish(
                self.channel_identifiers[queue_name],
                str(
                    {
                        "identifier": item["identifier"],
                        "exception": exception_str,
                    }
                ),
            )
