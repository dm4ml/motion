import logging
import threading
import time

from croniter import croniter
from datetime import datetime
from motion.trigger import TriggerElement, TriggerFn


class CronThread(threading.Thread):
    """Thread that executes a task on cron schedule."""

    def __init__(
        self, cron_expression, cursor, trigger_fn: TriggerFn, checkpoint_fn
    ):
        threading.Thread.__init__(self)
        self.daemon = True
        self.cron_expression = cron_expression
        self.cur = cursor
        self.trigger_fn = trigger_fn
        self.checkpoint_fn = checkpoint_fn
        self.running = True
        self.first_run = True

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
            else:
                self.first_run = False

            # Run trigger
            trigger_elem = TriggerElement(
                namespace=None, key=self.cron_expression, value=None
            )

            self.cur.executeTrigger(
                identifier=None,
                trigger=self.trigger_fn,
                trigger_elem=trigger_elem,
            )
            self.cur.waitForResults()
            logging.info(
                f"Finished waiting for background task {self.trigger_fn.name}."
            )
            self.checkpoint_fn()
            logging.info(
                f"Checkpointed store from task {self.trigger_fn.name}."
            )

    def stop(self):
        logging.info(f"Stopping task thread for name {self.trigger_fn.name}")
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

            logging.info(f"Checkpointing store {self.store.name}")
            self.store.checkpoint()
            logging.info(f"Finished checkpointing store {self.store.name}")

    def stop(self):
        logging.info(f"Stopping checkpoint thread for store {self.store.name}")
        self.running = False
