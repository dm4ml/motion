import os
import threading
import typing

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from croniter import croniter

from motion import Schema, Trigger
from motion.cursor import Cursor
from motion.task import CheckpointThread, CronThread
from motion.utils import TriggerFn, logger


class Store:
    def __init__(
        self,
        name: str,
        session_id: str,
        datastore_prefix: str = "datastores",
        checkpoint: str = "0 * * * *",
        disable_cron_triggers: bool = False,
    ) -> None:
        self.name = name
        self.session_id: str = session_id

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
            self.relations,
            self.table_columns,
            self.log_table,
        ) = self.loadFromCheckpoint_pa()
        if self.log_table is None:
            self.addLogTable_pa()

        # Set up triggers
        self.triggers: typing.Dict[str, list[TriggerFn]] = {}
        self.cron_triggers: typing.Dict[str, list[TriggerFn]] = {}
        self.cron_threads: typing.Dict[str, CronThread] = {}
        self.trigger_names: typing.Dict[str, list[str]] = {}
        self.trigger_fns: typing.Dict[str, Trigger] = {}

    def __del__(self) -> None:
        self.stop(wait=False)

    @property
    def listening(self) -> bool:
        return self._listening

    def cursor(
        self, bypass_listening: bool = False, wait_for_results: bool = False
    ) -> Cursor:
        """Generates a new cursor for the database, with triggers and all.

        Returns:
            Cursor: The cursor.
        """
        if not self.listening and not bypass_listening:
            raise Exception(
                "Store has not started. Call store.start() before using the cursor."
            )
        return Cursor(
            name=self.name,
            relations=self.relations,
            log_table=self.log_table,
            table_columns=self.table_columns,
            triggers=self.triggers,
            write_lock=self.db_write_lock,
            session_id=self.session_id,
            wait_for_results=wait_for_results,
        )

    def checkpoint_pa(self) -> None:
        """Checkpoint store object."""
        # Save relations
        base_path = os.path.join(self.datastore_prefix, self.name)
        for relation in self.relations:
            os.makedirs(os.path.join(base_path, relation), exist_ok=True)
            ds.write_dataset(
                self.relations[relation],
                base_dir=os.path.join(base_path, relation),
                format="parquet",
                partitioning=ds.partitioning(
                    pa.schema([("session_id", pa.string())]), flavor="hive"
                ),
                existing_data_behavior="delete_matching",
                schema=self.relations[relation].schema,
            )

        # Save logs
        pq.write_table(self.log_table, os.path.join(base_path, "logs.parquet"))

        # TODO: checkpoint trigger objects

    def loadFromCheckpoint_pa(self) -> tuple[dict, dict, pa.Table]:
        """Load store object from checkpoint."""
        try:
            relations = {}
            table_columns = {}

            base_path = os.path.join(self.datastore_prefix, self.name)

            # Iterate through all folders in base_path
            for relation in os.listdir(base_path):
                if relation == "logs.parquet":
                    continue

                dataset = ds.dataset(
                    os.path.join(base_path, relation),
                    format="parquet",
                    partitioning="hive",
                )

                # Load session_id partition
                try:
                    table = dataset.to_table(
                        filter=pc.equal(ds.field("session_id"), self.session_id)
                    )
                except pa.ArrowInvalid as e:
                    # If no session_id partition exists, move on
                    continue

                relations[relation] = table

                # Load table columns
                table_columns[relation] = table.schema.names
                table_columns[relation].remove("identifier")
                table_columns[relation].remove("derived_id")

                logger.info(
                    f"Loaded relation {relation} from checkpoint with {table.num_rows} existing rows in session."
                )

            # Load logs
            log_table = pq.read_table(os.path.join(base_path, "logs.parquet"))

            return relations, table_columns, log_table

        except Exception as e:
            logger.warning(
                f"Could not load database {self.name} from checkpoint. Error: {e}"
            )
            return {}, {}, None

        # TODO: load trigger objects

    def addLogTable_pa(self) -> None:
        """Creates a table to store trigger logs."""

        schema = pa.schema(
            [
                pa.field(
                    "executed_time",
                    pa.timestamp("us"),
                    nullable=False,
                ),
                pa.field("session_id", pa.string(), nullable=False),
                pa.field("trigger_name", pa.string(), nullable=False),
                pa.field("trigger_version", pa.int64(), nullable=False),
                pa.field("trigger_action", pa.string(), nullable=False),
                pa.field("relation", pa.string(), nullable=False),
                pa.field("identifier", pa.string(), nullable=False),
                pa.field("trigger_key", pa.string(), nullable=False),
            ]
        )
        # Create table with schema
        self.log_table = schema.empty_table()

    def addrelation_pa(self, name: str, schema: Schema) -> None:
        """_Add a relation to the store.

        Args:
            name (str): The name of the relation.
            schema (motion.Schema): The schema of the relation.
        """
        pa_schema = schema.formatPaSchema(name)

        if name in self.relations:
            pa_schema_names = pa_schema.names
            pa_schema_types = pa_schema.types

            for old_name, old_type in zip(
                self.relations[name].schema.names,
                self.relations[name].schema.types,
            ):
                if old_name == "session_id":
                    continue
                name_idx = pa_schema_names.index(old_name)
                if not old_type.equals(pa_schema_types[name_idx]):
                    logger.error(
                        f"relation {name} already exists with a different schema. Please clear the data store with `motion clear {self.name}` and try again."
                    )

        else:
            logger.info(f"Adding relation {name} with schema {pa_schema}")
            self.relations[name] = pa_schema.empty_table()

            self.table_columns[name] = self.relations[name].schema.names
            self.table_columns[name].remove("identifier")
            self.table_columns[name].remove("derived_id")

    def addTrigger(
        self,
        name: str,
        trigger: typing.Type[Trigger],
        params: typing.Dict[str, typing.Any] = {},
    ) -> None:
        """Adds a trigger to the store.

        Args:
            name (str): Trigger name.
            trigger (Trigger): Trigger class to execute when trigger is fired. Must implement the Trigger interface.
            params (typing.Dict[str, typing.Any], optional): Parameters to pass

        Raises:
            ValueError: If there is already a trigger with the given name.
        """
        if name in self.trigger_names:
            logger.warning(f"Trigger {name} already exists. Doing nothing.")
            return

        # Check that the class implements the Trigger interface
        if not issubclass(trigger, Trigger):
            raise ValueError(f"Trigger class must implement the Trigger interface.")

        # Retrieve keys
        keys = trigger.getRouteKeys()

        # Check that keys are valid
        all_possible_keys = [
            f"{ns}.{key}" for ns in self.table_columns for key in self.table_columns[ns]
        ]
        cron_key_exists = False
        for full_key in keys:
            _, key = full_key.split(".")
            if full_key not in all_possible_keys and not croniter.is_valid(key):
                raise ValueError(
                    f"Trigger {name} has invalid key {full_key}. Valid keys are {all_possible_keys} or a cron expression. If your schemas have changed, you may need to clear your application by running `motion clear {self.name}`."
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

        trigger_exec = trigger(
            self.cursor(bypass_listening=True), name, version, params
        )
        self.trigger_fns[name] = trigger_exec

        for full_key in keys:
            _, key = full_key.split(".")
            if croniter.is_valid(key):
                self.cron_triggers[full_key] = self.cron_triggers.get(full_key, []) + [
                    TriggerFn(name, trigger_exec)
                ]

            else:
                self.triggers[full_key] = self.triggers.get(full_key, []) + [
                    TriggerFn(name, trigger_exec)
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
                self.cron_triggers[key].remove(TriggerFn(name, fn))
                self.cron_threads[name].stop()
                self.cron_threads[name].join()

            else:
                self.triggers[key].remove(TriggerFn(name, fn))

        del self.trigger_names[name]
        del self.trigger_fns[name]

    def getTriggersForKey(self, relation: str, key: str) -> list[str]:
        """Get the list of triggers for a given key.

        Args:
            relation (str): The relation to get the triggers for.
            key (str): The key to get the triggers for.

        Returns:
            typing.List[str]: The list of triggers for the given key.
        """
        names_and_fns = self.triggers.get(f"{relation}.{key}", [])
        return [t[0] for t in names_and_fns]

    def getTriggersForAllKeys(self) -> typing.Dict[str, list[str]]:
        """Get the list of triggers for all keys.

        Returns:
            typing.Dict[str, typing.List[str]]: The list of triggers for all keys.
        """
        return {
            k: self.getTriggersForKey(k.split(".")[0], k.split(".")[1])
            for k in self.triggers.keys()
        }

    def start(self) -> None:
        """Start the store."""
        # Start cron triggers
        self._listening = True
        self.cron_threads = {}

        if not self.disable_cron_triggers:
            for full_key, triggers in self.cron_triggers.items():
                _, cron_expression = full_key.split(".")
                for trigger_fn in triggers:
                    e = threading.Event()
                    t = CronThread(
                        cron_expression,
                        self.cursor(wait_for_results=True),
                        trigger_fn,
                        self.checkpoint_pa,
                        e,
                        self.session_id,
                    )
                    self.cron_threads[trigger_fn.name] = t
                    t.start()

        # Start a thread to checkpoint the store every 5 minutes
        self.checkpoint_thread = CheckpointThread(
            self.name, self.checkpoint_pa, self.checkpoint_interval
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
            raise FileNotFoundError(
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
