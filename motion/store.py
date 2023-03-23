import dill
import duckdb
import inspect
import logging
import os
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import threading
import typing
import uuid

from croniter import croniter
from enum import Enum
from motion import Trigger, Schema
from motion.dbcon import Connection
from motion.task import CronThread, CheckpointThread
from motion.trigger import TriggerFn

from motion.utils import logger


class Store(object):
    def __init__(
        self,
        name: str,
        datastore_prefix: str = "datastores",
        checkpoint: str = "0 * * * *",
        disable_cron_triggers: bool = False,
        prod: bool = False,
    ):
        self.name = name
        self.session_id = "PROD" if prod else str(uuid.uuid4())
        self.datastore_prefix = datastore_prefix
        self.checkpoint_interval = checkpoint
        self.disable_cron_triggers = disable_cron_triggers

        # Set listening to false
        self._listening = False

        # self.con = duckdb.connect(":memory:")
        if not os.path.exists(os.path.join(datastore_prefix, self.name)):
            os.makedirs(os.path.join(datastore_prefix, self.name))

        self.db_write_lock = threading.Lock()

        # Try loading from checkpoint
        (
            self.namespaces,
            self.table_columns,
            self.log_table,
        ) = self.loadFromCheckpoint_pa()
        if self.log_table is None:
            self.addLogTable_pa()

        # Set up triggers
        self.triggers = {}
        self.cron_triggers = {}
        self.cron_threads = {}
        self.trigger_names = {}
        self.trigger_fns = {}

    def __del__(self):
        self.stop(wait=False)

    @property
    def listening(self):
        return self._listening

    def cursor(
        self, bypass_listening: bool = False, wait_for_results: bool = False
    ):
        """Generates a new cursor for the database, with triggers and all.

        Returns:
            Connection: The cursor.
        """
        if not self.listening and not bypass_listening:
            raise Exception(
                "Store has not started. Call store.start() before using the cursor."
            )

        return Connection(
            self.name,
            self.namespaces,
            self.log_table,
            self.table_columns,
            self.triggers,
            self.db_write_lock,
            self.session_id,
            wait_for_results,
        )

    def checkpoint_pa(self):
        """Checkpoint store object."""
        # try:
        # Save namespaces
        base_path = os.path.join(self.datastore_prefix, self.name)
        for namespace in self.namespaces:
            os.makedirs(os.path.join(base_path, namespace), exist_ok=True)
            ds.write_dataset(
                self.namespaces[namespace],
                base_dir=os.path.join(base_path, namespace),
                format="parquet",
                partitioning=["session_id"],
                existing_data_behavior="delete_matching",
                schema=self.namespaces[namespace].schema,
            )

        # Save logs
        pq.write_table(self.log_table, os.path.join(base_path, "logs.parquet"))

        # TODO: checkpoint trigger objects

    def loadFromCheckpoint_pa(self):
        """Load store object from checkpoint."""
        try:
            namespaces = {}
            table_columns = {}

            base_path = os.path.join(self.datastore_prefix, self.name)
            # Iterate through all folders in base_path
            for namespace in os.listdir(base_path):
                if namespace == "logs.parquet":
                    continue

                dataset = ds.dataset(os.path.join(base_path, namespace))

                # Load session_id partition
                table = dataset.to_table(
                    filter=ds.field("session_id") == self.session_id
                )
                namespaces[namespace] = table

                # Load table columns
                table_columns[namespace] = table.schema.names
                table_columns[namespace].remove("identifier")
                table_columns[namespace].remove("derived_id")

                logger.info(
                    f"Loaded namespace {namespace} from checkpoint with {table.num_rows} existing rows in session."
                )

            # Load logs
            log_table = pq.read_table(os.path.join(base_path, "logs.parquet"))

            return namespaces, table_columns, log_table

        except Exception as e:
            logger.warning(
                f"Could not load database {self.name} from checkpoint. Error: {e}"
            )
            return {}, {}, None

        # TODO: load trigger objects

    def addLogTable_pa(self):
        """Creates a table to store trigger logs."""

        schema = pa.schema(
            [
                pa.field(
                    "executed_time",
                    pa.timestamp("ns"),
                    nullable=False,
                ),
                pa.field("session_id", pa.string(), nullable=False),
                pa.field("trigger_name", pa.string(), nullable=False),
                pa.field("trigger_version", pa.int64(), nullable=False),
                pa.field("trigger_action", pa.string(), nullable=False),
                pa.field("namespace", pa.string(), nullable=False),
                pa.field("identifier", pa.string(), nullable=False),
                pa.field("trigger_key", pa.string(), nullable=False),
            ]
        )
        # Create table with schema
        self.log_table = schema.empty_table()

    def addNamespace_pa(self, name: str, schema: Schema) -> None:
        """_Add a namespace to the store.

        Args:
            name (str): The name of the namespace.
            schema (motion.Schema): The schema of the namespace.
        """
        pa_schema = schema.formatPaSchema(name)

        if name in self.namespaces:
            if self.namespaces[name].schema != pa_schema:
                logger.error(
                    f"Namespace {name} already exists with a different schema. Please clear the data store with `motion clear {self.name}` and try again."
                )

        else:
            logger.info(f"Adding namespace {name} with schema {pa_schema}")
            self.namespaces[name] = pa_schema.empty_table()

            self.table_columns[name] = self.namespaces[name].schema.names
            self.table_columns[name].remove("identifier")
            self.table_columns[name].remove("derived_id")

    def addTrigger(
        self,
        name: str,
        keys: typing.List[str],
        trigger: typing.Union[typing.Callable, type],
        params: typing.Dict[str, typing.Any] = {},
    ) -> None:
        """Adds a trigger to the store.

        Args:
            name (str): Trigger name.
            keys (typing.List[str]): Names of the keys to triger on. Formatted
            as "namespace.key" or cron expression. Trigger executes if there is
            a addition to any of the keys, or on the cron schedule.
            trigger (typing.Union[typing.Callable, type]): Function or class to
            execute when the trigger is fired. If function, must take in the id
            of the row that triggered the trigger, a reference to the element
            that triggered it, and a reference to the store object (in this
            order). If class, must implement the Transform interface.
            params (typing.Dict[str, typing.Any], optional): Parameters to pass

        Raises:
            ValueError: If there is already a trigger with the given name.
        """
        if name in self.trigger_names:
            logger.warning(f"Trigger {name} already exists. Doing nothing.")
            return

        if inspect.isfunction(trigger):
            # Check that the function signature is correct
            if len(inspect.signature(trigger).parameters) != 2:
                raise ValueError(
                    f"Trigger function must take in 2 arguments: cursor and triggered_by."
                )

        elif inspect.isclass(trigger):
            # Check that the class implements the Transform interface
            if not issubclass(trigger, Trigger):
                raise ValueError(
                    f"Trigger class must implement the Trigger interface."
                )

        else:
            raise ValueError(
                f"Trigger {name} must be a function or class. Got {type(trigger)}."
            )

        # Check that keys are valid
        all_possible_keys = [
            f"{ns}.{key}"
            for ns in self.table_columns
            for key in self.table_columns[ns]
        ]
        cron_key_exists = False
        for key in keys:
            if key not in all_possible_keys and not croniter.is_valid(key):
                raise ValueError(
                    f"Trigger {name} has invalid key {key}. Valid keys are {all_possible_keys} or a cron expression. If your schemas have changed, you may need to clear your application by running `motion clear {self.name}`."
                )

            if croniter.is_valid(key):
                if cron_key_exists:
                    raise ValueError(
                        f"Trigger {name} has more than one cron key. Only one cron key is allowed per trigger."
                    )

                cron_key_exists = True

        # Add the trigger to the store
        self.trigger_names[name] = keys

        version = pc.max(
            pc.filter(
                self.log_table["trigger_version"],
                pc.equal(self.log_table["trigger_name"], name),
            )
        ).as_py()

        version = version if version is not None else 0

        trigger_exec = (
            trigger(self.cursor(bypass_listening=True), name, version, params)
            if inspect.isclass(trigger)
            else trigger
        )
        self.trigger_fns[name] = trigger_exec

        for key in keys:
            if croniter.is_valid(key):
                self.cron_triggers[key] = self.cron_triggers.get(key, []) + [
                    TriggerFn(name, trigger_exec, inspect.isclass(trigger))
                ]

            else:
                self.triggers[key] = self.triggers.get(key, []) + [
                    TriggerFn(name, trigger_exec, inspect.isclass(trigger))
                ]

    def deleteTrigger(self, name: str) -> None:
        """Delete a trigger from the store.

        Args:
            name (str): The name of the trigger.
        """
        if name not in self.trigger_names:
            raise ValueError(f"Trigger {name} does not exist.")

        # Remove the trigger from the store
        keys = self.trigger_names[name]
        fn = self.trigger_fns[name]
        for key in keys:
            if croniter.is_valid(key):
                self.cron_triggers[key].remove(
                    (name, fn, isinstance(fn, Trigger))
                )
                self.cron_threads[name].stop()
                self.cron_threads[name].join()

            else:
                self.triggers[key].remove((name, fn, isinstance(fn, Trigger)))

        del self.trigger_names[name]
        del self.trigger_fns[name]

    def getTriggersForKey(self, namespace: str, key: str) -> typing.List[str]:
        """Get the list of triggers for a given key.

        Args:
            namespace (str): The namespace to get the triggers for.
            key (str): The key to get the triggers for.

        Returns:
            typing.List[str]: The list of triggers for the given key.
        """
        names_and_fns = self.triggers.get(f"{namespace}.{key}", [])
        return [t[0] for t in names_and_fns]

    def getTriggersForAllKeys(self) -> typing.Dict[str, typing.List[str]]:
        """Get the list of triggers for all keys.

        Returns:
            typing.Dict[str, typing.List[str]]: The list of triggers for all keys.
        """
        return {k: self.getTriggersForKey(k) for k in self.triggers.keys()}

    def start(self) -> None:
        """Start the store."""
        # Start cron triggers
        self._listening = True
        self.cron_threads = {}

        if not self.disable_cron_triggers:
            for cron_expression, triggers in self.cron_triggers.items():
                for trigger_fn in triggers:
                    e = threading.Event()
                    t = CronThread(
                        cron_expression,
                        self.cursor(wait_for_results=True),
                        trigger_fn,
                        self.checkpoint_pa,
                        e,
                    )
                    self.cron_threads[trigger_fn.name] = t
                    t.start()

        # Start a thread to checkpoint the store every 5 minutes
        self.checkpoint_thread = CheckpointThread(
            self, self.checkpoint_interval
        )
        self.checkpoint_thread.start()

    def waitForTrigger(self, trigger_name: str) -> None:
        """Wait for a cron-scheduled trigger to fire.

        Args:
            trigger_name (str): The name of the trigger to wait for.
        """
        if self.disable_cron_triggers:
            raise ValueError(
                f"Cannot wait for trigger {trigger_name} because cron triggers are disabled."
            )

        if trigger_name not in self.cron_threads.keys():
            raise ValueError(
                f"Trigger {trigger_name} does not exist as a cron-scheduled thread. Valid cron-scheduled triggers are {list(self.cron_threads.keys())}."
            )

        logger.info(f"Waiting for trigger {trigger_name} to fire...")
        self.cron_threads[trigger_name].first_run_event.wait()

    def stop(self, wait: bool = False) -> None:
        """Stop the store."""
        # Stop cron triggers

        if self._listening:
            for _, t in self.cron_threads.items():
                t.stop()
                if wait:
                    t.join()

            self.checkpoint_thread.stop()
            if wait:
                self.checkpoint_thread.join()

        self._listening = False

        logger.info("Stopped store.")
