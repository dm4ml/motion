import threading
import time
import typing
from datetime import datetime

from croniter import croniter

from motion.cursor import Cursor
from motion.utils import PRODUCTION_SESSION_ID, TriggerElement, TriggerFn, logger


class CronThread(threading.Thread):
    """Thread that executes a task on cron schedule."""

    def __init__(
        self,
        cron_expression: str,
        cursor: Cursor,
        trigger_fn: TriggerFn,
        checkpoint_fn: typing.Callable,
        first_run_event: threading.Event,
        session_id: str,
    ) -> None:
        threading.Thread.__init__(self)
        self.daemon = True
        self.cron_expression = cron_expression
        self.cur = cursor
        self.trigger_fn = trigger_fn
        self.checkpoint_fn = checkpoint_fn
        self.running = True
        self.first_run = True
        self.first_run_event = first_run_event
        self.session_id = session_id

    def run(self) -> None:
        while self.running:
            next_time = croniter(self.cron_expression, datetime.now()).get_next(
                datetime
            )
            delay = (next_time - datetime.now()).total_seconds()

            # Wait until the scheduled time
            if not self.first_run:
                if delay > 0:
                    time.sleep(delay)
                else:
                    continue

            # Run trigger
            trigger_context = TriggerElement(
                relation="_cron",
                identifier="SCHEDULED",
                key=self.cron_expression,
                value=None,
            )

            try:
                self.cur.executeTrigger(
                    trigger=self.trigger_fn,
                    trigger_context=trigger_context,
                )

                self.cur.waitForResults()
                logger.info(
                    f"Finished waiting for background task {self.trigger_fn.name}."
                )
            except Exception as e:
                if self.session_id == PRODUCTION_SESSION_ID:
                    logger.error(
                        f"Error while running task {self.trigger_fn.name}: {e}"
                    )
                    continue
                else:
                    if self.first_run:
                        self.first_run_event.set()
                    raise e

            if self.first_run:
                self.first_run_event.set()
                self.first_run = False

            self.checkpoint_fn()
            logger.debug(f"Checkpointed store from task {self.trigger_fn.name}.")

    def stop(self) -> None:
        logger.debug(f"Stopping task thread for name {self.trigger_fn.name}")
        self.running = False


class CheckpointThread(threading.Thread):
    def __init__(
        self,
        store_name: str,
        checkpoint_fn: typing.Callable,
        cron_expression: str,
    ) -> None:
        threading.Thread.__init__(self)
        self.name = store_name
        self.daemon = True
        self.checkpoint_fn = checkpoint_fn
        self.running = True
        self.cron_expression = cron_expression

    def run(self) -> None:
        while self.running:
            next_time = croniter(self.cron_expression, datetime.now()).get_next(
                datetime
            )
            delay = (next_time - datetime.now()).total_seconds()
            if delay > 0:
                time.sleep(delay)
            else:
                continue

            logger.debug(f"Checkpointing store {self.name}")
            self.checkpoint_fn()
            logger.debug(f"Finished checkpointing store {self.name}")

    def stop(self) -> None:
        logger.debug(f"Stopping checkpoint thread for store {self.name}")
        self.running = False
