"""
Database connection, with functions that a users is allowed to
call within trigger lifecycle methods.
"""
import logging
import pandas as pd
import threading
import typing

from enum import Enum
from motion.trigger import TriggerElement, TriggerFn


class Connection(object):
    def __init__(
        self,
        name,
        db_con,
        table_columns,
        triggers,
        write_lock,
        wait_for_results=False,
    ):
        self.name = name
        self.cur = db_con.cursor()
        self.table_columns = table_columns
        self.triggers = triggers
        self.write_lock = write_lock
        self.wait_for_results = wait_for_results
        self.fit_events = []

    def __del__(self):
        if self.wait_for_results:
            self.waitForResults()

    def waitForResults(self):
        for t in self.fit_events:
            t.wait()
        self.fit_events = []

    def getNewId(self, namespace: str, key: str = "identifier") -> int:
        """Get a new id for a namespace.

        Args:
            namespace (str): The namespace to get the new id for.
            key (str, optional): The key to get the new id for. Defaults to "id".

        Returns:
            int: The new id.
        """

        self.cur.execute(
            f"CREATE SEQUENCE IF NOT EXISTS {self.name}.{namespace}_{key}_seq;"
        )
        new_id = self.cur.execute(
            f"SELECT NEXTVAL('{self.name}.{namespace}_{key}_seq')"
        ).fetchone()[0]
        # logging.info(f"New id for {namespace} is {new_id}")
        return new_id

    def exists(self, namespace: str, identifier: int) -> bool:
        """Determine if a record exists in a namespace.

        Args:
            namespace (str): The namespace to check.
            identifier (int): The primary key of the record.

        Returns:
            bool: True if the record exists, False otherwise.
        """
        elem = self.cur.execute(
            f"SELECT identifier FROM {self.name}.{namespace} WHERE identifier = {identifier}"
        ).fetchone()
        return elem is not None

    def set(
        self,
        namespace: str,
        identifier: int,
        key_values: typing.Dict[str, typing.Any],
        run_duplicate_triggers: bool = False,
    ) -> int:
        """Set multiple values for a key in a namespace.
        TODO(shreyashankar): Handle complex types.

        Args:
            namespace (str): The namespace to set the value in.
            identifier (int): The id of the record to set the value for.
            key_values (typing.Dict[str, typing.Any]): The key-value pairs to set.
            run_duplicate_triggers (bool, optional): Whether to run duplicate triggers. Defaults to False.
        """
        if not identifier:
            identifier = self.getNewId(namespace)

        # Convert enums to their values
        for key, value in key_values.items():
            if isinstance(value, Enum):
                key_values.update({key: value.value})

        if not self.exists(namespace, identifier):
            with self.write_lock:
                query_string = (
                    f"INSERT INTO {self.name}.{namespace} (identifier, {', '.join(key_values.keys())}) VALUES (?, {', '.join(['?'] * len(key_values.keys()))})",
                    (identifier, *key_values.values()),
                )
                self.cur.execute(*query_string)

        else:
            # Delete and re-insert the row with the new value
            old_row = self.cur.execute(
                f"SELECT * FROM {self.name}.{namespace} WHERE identifier = {identifier}"
            ).fetch_df()
            # self.cur.execute(
            #     f"DELETE FROM {self.name}.{namespace} WHERE id = ?;", (id,)
            # )
            # logging.info(f"Deleted row {id} from {namespace}.")

            # Update the row with the new value
            for key, value in key_values.items():
                old_row.at[0, key] = value

            with self.write_lock:
                self.cur.execute(
                    f"""DELETE FROM {self.name}.{namespace} WHERE identifier = {identifier}; INSERT INTO {self.name}.{namespace} SELECT * FROM old_row;""",
                )

        # Run triggers
        executed = set()
        for key, value in key_values.items():
            trigger_elem = TriggerElement(
                namespace=namespace, key=key, value=value
            )
            for trigger in self.triggers.get(f"{namespace}.{key}", []):
                if run_duplicate_triggers or trigger not in executed:
                    self.executeTrigger(identifier, trigger, trigger_elem)
                    executed.add(trigger)

        return identifier

    def logTriggerExecution(
        self,
        trigger_name,
        trigger_version,
        trigger_action,
        namespace,
        identifier,
        trigger_key,
    ):
        """Logs a trigger execution.

        Args:
            trigger_name (str): The name of the trigger.
            trigger_version (int): The version of the trigger.
            trigger_action (str): The action of the trigger.
            namespace (str): The namespace of the trigger.
            identifier (int): The id of the trigger.
            trigger_key (str): The key of the trigger.
        """

        self.cur.execute(
            f"INSERT INTO {self.name}.logs(trigger_name, trigger_version, trigger_action, namespace, identifier, trigger_key) VALUES (?, ?, ?, ?, ?, ?)",
            (
                trigger_name,
                trigger_version,
                trigger_action,
                namespace,
                identifier,
                trigger_key,
            ),
        )

    def executeTrigger(
        self,
        identifier: int,
        trigger: TriggerFn,
        trigger_elem: TriggerElement,
    ):
        """Execute a trigger.

        Args:
            identifier (int): The identifier of the record that triggered the trigger.
            trigger (TriggerFn): The trigger to execute.
            trigger_elem (TriggerElement): The element that triggered the trigger.
        """
        trigger_name, trigger_fn, isTransform = trigger
        logging.info(
            f"Running trigger {trigger_name} for identifier {identifier}, key {trigger_elem.key}..."
        )
        new_connection = Connection(
            self.name,
            self.cur,
            self.table_columns,
            self.triggers,
            self.write_lock,
            self.wait_for_results,
        )

        if not isTransform:
            trigger_fn(
                new_connection,
                identifier,
                trigger_elem,
            )
            # Log the trigger execution
            self.logTriggerExecution(
                trigger_name,
                0,
                "function",
                trigger_elem.namespace,
                identifier,
                trigger_elem.key,
            )

            logging.info(
                f"Finished running trigger {trigger_name} for identifier {identifier}."
            )

        else:
            # Execute the transform lifecycle
            if trigger_fn.shouldInfer(
                new_connection,
                identifier,
                trigger_elem,
            ):
                trigger_fn.infer(
                    new_connection,
                    identifier,
                    trigger_elem,
                )
                self.logTriggerExecution(
                    trigger_name,
                    trigger_fn.version,
                    "infer",
                    trigger_elem.namespace,
                    identifier,
                    trigger_elem.key,
                )

            if trigger_fn.shouldFit(
                new_connection,
                identifier,
                trigger_elem,
            ):
                fit_thread = trigger_fn.fitWrapper(
                    new_connection,
                    trigger_name,
                    identifier,
                    trigger_elem,
                )
                self.fit_events.append(fit_thread)
            else:
                logging.info(
                    f"Finished running trigger {trigger_name} for identifier {identifier}."
                )

    def duplicate(self, namespace: str, identifier: int) -> int:
        """Duplicate a record in a namespace. Doesn't run triggers.

        Args:
            namespace (str): The namespace to duplicate the record in.
            identifier (int): The identifier of the record to duplicate.

        Returns:
            int: The new identifier of the duplicated record.
        """
        new_id = self.getNewId(namespace)

        with self.write_lock:
            self.cur.execute(
                f"INSERT INTO {self.name}.{namespace} SELECT {new_id} AS identifier, {identifier} AS derived_id, {', '.join(self.table_columns[namespace])} FROM {self.name}.{namespace} WHERE identifier = {identifier}"
            )

        return new_id

    def get(
        self, namespace: str, identifier: int, keys: typing.List[str], **kwargs
    ) -> typing.Any:
        """Get values for an identifier's keys in a namespace.
        TODO: Handle complex types.

        Args:
            namespace (str): The namespace to get the value from.
            identifier (int): The identifier of the record to get the value for.
            keys (typing.List[str]): The keys to get the values for.

        Keyword Args:
            caller_id (int, optional): The identifier of the caller. Defaults to None.
            Used to prevent leakage, i.e., looking at data that has not
            been generated yet.
            include_derived (bool, optional): Whether to include derived ids. Defaults to False.
            filter_null (bool, optional): Whether to filter out null values. Only used in conjuction with include_derived. Defaults to True.

        Returns:
            typing.Any: The values for the keys.
        """
        # Check that there is no leakage
        if kwargs.get("caller_id") is not None:
            caller_id = kwargs.get("caller_id")
            if caller_id > identifier:
                raise ValueError(
                    f"Caller id {caller_id} is greater than identifier {identifier}!"
                )

        if not kwargs.get("include_derived", False):
            res = self.cur.execute(
                f"SELECT {', '.join(keys)} FROM {self.name}.{namespace} WHERE identifier = {identifier}"
            ).fetchone()
            res_dict = {k: v for k, v in zip(keys, res)}
            res_dict.update({"identifier": identifier})
            return res_dict

        # Recursively get derived ids
        id_res = self.cur.execute(
            f"SELECT identifier FROM {self.name}.{namespace} WHERE derived_id = {identifier}"
        ).fetchall()
        id_res = [i[0] for i in id_res]
        all_ids = [identifier] + id_res
        while len(id_res) > 0:
            id_res = self.cur.execute(
                f"SELECT identifier FROM {self.name}.{namespace} WHERE derived_id IN ({', '.join([str(i) for i in id_res])})"
            ).fetchall()
            id_res = [i[0] for i in id_res]
            all_ids.extend(id_res)

        if kwargs.get("filter_null", True):
            return self.cur.execute(
                f"SELECT identifier, {', '.join(keys)} FROM {self.name}.{namespace} WHERE identifier IN ({', '.join([str(i) for i in all_ids])}) AND {' AND '.join([f'{k} IS NOT NULL' for k in keys])}"
            ).fetchdf()

        else:
            return self.cur.execute(
                f"SELECT identifier, {', '.join(keys)} FROM {self.name}.{namespace} WHERE identifier IN ({', '.join([str(i) for i in all_ids])})"
            ).fetchdf()

    def mget(
        self,
        namespace: str,
        identifiers: typing.List[int],
        keys: typing.List[str],
        **kwargs,
    ) -> pd.DataFrame:
        """Get multiple values for keys in a namespace.
        TODO: Handle complex types.

        Args:
            namespace (str): The namespace to get the value from.
            identifiers (typing.List[int]): The ids of the records to get the value for.
            keys (typing.List[str]): The keys to get the values for.
            filter_null (bool, optional): Whether to filter out null values.  Defaults to True.

        Keyword Args:
            caller_id (int, optional): The identifier of the caller. Defaults to None.
            Used to prevent leakage, i.e., looking at data that has not been
            generated yet.

        Returns:
            pd.DataFrame: The values for the key.
        """
        # Check that there is no leakage
        if kwargs.get("caller_id") is not None:
            caller_id = kwargs.get("caller_id")
            if caller_id > max(identifiers):
                raise ValueError(
                    f"Caller identifier {caller_id} is greater than id {max(identifiers)}!"
                )

        res = self.cur.execute(
            f"SELECT identifier, {', '.join(keys)} FROM {self.name}.{namespace} WHERE identifier IN ({', '.join([str(identifier) for identifier in identifiers])})"
        ).fetchdf()

        if kwargs.get("filter_null", True):
            return res.dropna().reset_index(drop=True)

        else:
            return res

    def getIdsForKey(
        self, namespace: str, key: str, value: typing.Any, **kwargs
    ) -> typing.List[int]:
        """Get ids for a key-value pair in a namespace.

        Args:
            namespace (str): The namespace to get the value from.
            key (str): The key to get the values for.
            value (typing.Any): The value to get the ids for.

        Keyword Args:
            caller_id (int, optional): The id of the caller. Defaults to None.
            caller_namespace (str, optional): The namespace of the caller. Defaults to None.

        Returns:
            typing.List[int]: The ids for the key-value pair.
        """
        # Retrieve caller_id if it exists
        caller_id = kwargs.get("caller_id", None)
        caller_namespace = kwargs.get("caller_namespace", None)
        if caller_id is not None and caller_namespace is not None:
            caller_time = self.cur.execute(
                f"SELECT ts FROM {self.name}.{caller_namespace} WHERE identifier = ?",
                (caller_id,),
            ).fetchone()[0]
            res = self.cur.execute(
                f"SELECT identifier FROM {self.name}.{namespace} WHERE {key} = ? AND ts < ?",
                (value, caller_time),
            ).fetchall()
            return [r[0] for r in res]

        # Otherwise, just return all the ids
        res = self.cur.execute(
            f"SELECT identifier FROM {self.name}.{namespace} WHERE {key} = ?",
            (value,),
        ).fetchall()
        return [r[0] for r in res]

    def sql(self, stmt: str) -> pd.DataFrame:
        return self.cur.execute(stmt).fetchdf()
