import logging
import threading
import time

from croniter import croniter
from datetime import datetime
from motion.trigger import TriggerElement, TriggerFn

from motion.utils import logger


class CronThread(threading.Thread):
    """Thread that executes a task on cron schedule."""

    def __init__(
        self,
        cron_expression,
        cursor,
        trigger_fn: TriggerFn,
        checkpoint_fn,
        first_run_event: threading.Event,
    ):
        threading.Thread.__init__(self)
        self.daemon = True
        self.cron_expression = cron_expression
        self.cur = cursor
        self.trigger_fn = trigger_fn
        self.checkpoint_fn = checkpoint_fn
        self.running = True
        self.first_run = True
        self.first_run_event = first_run_event

    def run(self):
        while self.running:
            next_time = croniter(
                self.cron_expression, datetime.now()
            ).get_next(datetime)
            delay = (next_time - datetime.now()).total_seconds()

            # Wait until the scheduled time
            if not self.first_run:
                if delay > 0:
                    time.sleep(delay)
                else:
                    continue

            # Run trigger
            triggered_by = TriggerElement(
                namespace=None,
                identifier=None,
                key=self.cron_expression,
                value=None,
            )

            try:
                self.cur.executeTrigger(
                    trigger=self.trigger_fn,
                    triggered_by=triggered_by,
                )
                self.cur.waitForResults()
                logger.info(
                    f"Finished waiting for background task {self.trigger_fn.name}."
                )
            except Exception as e:
                logger.error(
                    f"Error while running task {self.trigger_fn.name}: {e}"
                )
                continue

            if self.first_run:
                self.first_run_event.set()
                self.first_run = False

            self.checkpoint_fn()
            logger.info(
                f"Checkpointed store from task {self.trigger_fn.name}."
            )

    def stop(self):
        logger.info(f"Stopping task thread for name {self.trigger_fn.name}")
        self.running = False


class CheckpointThread(threading.Thread):
    def __init__(self, store, cron_expression):
        threading.Thread.__init__(self)
        self.daemon = True
        self.store = store
        self.running = True
        self.cron_expression = cron_expression

    def run(self):
        while self.running:
            next_time = croniter(
                self.cron_expression, datetime.now()
            ).get_next(datetime)
            delay = (next_time - datetime.now()).total_seconds()
            if delay > 0:
                time.sleep(delay)
            else:
                continue

            logger.info(f"Checkpointing store {self.store.name}")
            self.store.checkpoint()
            logger.info(f"Finished checkpointing store {self.store.name}")

    def stop(self):
        logger.info(f"Stopping checkpoint thread for store {self.store.name}")
        self.running = False
