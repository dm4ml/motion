import asyncio
import traceback
from multiprocessing import Process
from threading import Thread
from typing import Any, Callable, Dict, List, Optional

import cloudpickle
import redis

from motion.route import Route
from motion.utils import loadState, logger, saveState


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
        redis_params: Dict[str, Any],
        running: Any,
    ):
        super().__init__()
        self.task_type = task_type
        self.name = f"UpdateTask-{task_type}-{instance_name}"
        self.instance_name = instance_name
        self.save_state_func = save_state_func
        self.load_state_func = load_state_func

        self.routes = routes
        self.queue_identifiers = queue_identifiers
        self.channel_identifiers = channel_identifiers

        self.running = running
        self.daemon = True

        self.redis_params = redis_params
        self.redis_con: Optional[redis.Redis] = None

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
            try:
                with redis_con.lock(f"MOTION_LOCK:{self.instance_name}", timeout=120):
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

            redis_con.publish(
                self.channel_identifiers[queue_name],
                str(
                    {
                        "identifier": item["identifier"],
                        "exception": exception_str,
                    }
                ),
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
