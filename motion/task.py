import threading
import time

from croniter import croniter
from datetime import datetime
from motion.trigger import TriggerElement, TriggerFn


class TaskThread(threading.Thread):
    """Thread that executes a task every N seconds"""

    def __init__(self, cron_expression, cursor, trigger_fn: TriggerFn):
        threading.Thread.__init__(self)
        self.daemon = True
        self.cron_expression = cron_expression
        self.cur = cursor
        self.trigger_fn = trigger_fn

    def run(self):
        while self.running:
            next_time = croniter(
                self.cron_expression, datetime.now()
            ).get_next(datetime)
            delay = (next_time - datetime.now()).total_seconds()

            # Wait until the scheduled time
            if delay > 0:
                time.sleep(delay)

            # sleep for interval or until shutdown
            trigger_elem = TriggerElement(
                namespace=None, key=self.cron_expression, value=None
            )

            self.cur.executeTrigger(
                id=None, trigger=self.trigger_fn, trigger_elem=trigger_elem
            )

    def stop(self):
        self.running = False
