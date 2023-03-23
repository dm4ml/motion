"""
Database connection, with functions that a users is allowed to
call within trigger lifecycle methods.
"""
import duckdb
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytz
import threading
import typing
import uuid

from datetime import datetime
from enum import Enum
from motion.trigger import TriggerElement, TriggerFn
from motion.utils import logger


class Connection(object):
    def __init__(
        self,
        name,
        namespaces,
        log_table,
        table_columns,
        triggers,
        write_lock,
        session_id,
        wait_for_results=False,
    ):
        self.name = name
        self.namespaces = namespaces
        self.log_table = log_table
        self.table_columns = table_columns
        self.triggers = triggers
        self.write_lock = write_lock
        self.session_id = session_id
        self.wait_for_results = wait_for_results
        self.fit_events = []

    def __del__(self):
        if self.wait_for_results:
            self.waitForResults()

    def waitForResults(self):
        for t in self.fit_events:
            t.wait()
        self.fit_events = []

    def getNewId(self, namespace: str, key: str = "identifier") -> str:
        """Get a new id for a namespace.

        Args:
            namespace (str): The namespace to get the new id for.
            key (str, optional): The key to get the new id for. Defaults to "id".

        Returns:
            str: The new id.
        """
        new_id = str(uuid.uuid4())

        # Check if the id already exists
        if self.exists(namespace, new_id):
            return self.getNewId(namespace, key)

        return new_id

    def exists(self, namespace: str, identifier: int) -> bool:
        """Determine if a record exists in a namespace.

        Args:
            namespace (str): The namespace to check.
            identifier (int): The primary key of the record.

        Returns:
            bool: True if the record exists, False otherwise.
        """

        # Check if identifier exists in pyarrow table
        table = self.namespaces[namespace]
        condition = pc.equal(table["identifier"], identifier)
        mask = pc.filter(table["identifier"], condition)
        result = len(mask) > 0

        return result

    def set(
        self,
        namespace: str,
        identifier: str,
        key_values: typing.Dict[str, typing.Any],
        run_duplicate_triggers: bool = False,
    ) -> str:
        """Set multiple values for a key in a namespace.
        TODO(shreyashankar): Handle complex types.

        Args:
            namespace (str): The namespace to set the value in.
            identifier (str): The id of the record to set the value for.
            key_values (typing.Dict[str, typing.Any]): The key-value pairs to set.
            run_duplicate_triggers (bool, optional): Whether to run duplicate triggers. Defaults to False.
        """
        if namespace is None:
            raise ValueError("Namespace cannot be None.")

        exists = True
        if not identifier:
            identifier = self.getNewId(namespace)
            exists = False

        if exists:
            exists = self.exists(namespace, identifier)

        # Convert enums to their values
        for key, value in key_values.items():
            if isinstance(value, Enum):
                key_values.update({key: value.value})

        # Insert or update based on identifier
        with self.write_lock:
            table = self.namespaces[namespace]

            if not exists:
                new_row_dict = {n: None for n in table.schema.names}
                new_row_dict.update(
                    {
                        "identifier": identifier,
                        "create_at": pd.Timestamp.now(),
                        "session_id": self.session_id,
                    }
                )
                new_row_dict.update(key_values)
                new_row_df = pd.DataFrame(new_row_dict, index=[0])

                new_row = pa.Table.from_pandas(new_row_df, schema=table.schema)
                final_table = pa.concat_tables([table, new_row])
                self.namespaces[namespace] = final_table

            else:
                condition = pc.equal(table["identifier"], identifier)
                row = pc.filter(table, condition).to_pandas()

                for key, value in key_values.items():
                    row.at[0, key] = value

                new_row = pa.Table.from_pandas(row, schema=table.schema)

                filtered_table = pc.filter(table, pc.invert(condition))
                final_table = pa.concat_tables([filtered_table, new_row])
                self.namespaces[namespace] = final_table

        # Run triggers
        executed = set()
        for key, value in key_values.items():
            triggered_by = TriggerElement(
                namespace=namespace,
                identifier=identifier,
                key=key,
                value=value,
            )
            for trigger in self.triggers.get(f"{namespace}.{key}", []):
                if run_duplicate_triggers or trigger not in executed:
                    self.executeTrigger(trigger, triggered_by)
                    executed.add(trigger)

        return identifier

    def logTriggerExecution(
        self, trigger_name, trigger_version, trigger_action, triggered_by
    ):
        """Logs a trigger execution.

        Args:
            trigger_name (str): The name of the trigger.
            trigger_version (int): The version of the trigger.
            trigger_action (str): The action of the trigger.
            triggered_by (TriggerElement): The element that triggered the trigger.
        """

        # Append to the log table
        new_row = {
            "executed_time": pd.Timestamp.now(),
            "session_id": self.session_id,
            "trigger_name": trigger_name,
            "trigger_version": trigger_version,
            "trigger_action": trigger_action,
            "namespace": triggered_by.namespace,
            "identifier": triggered_by.identifier,
            "trigger_key": triggered_by.key,
        }
        new_row = pa.Table.from_pandas(
            pd.DataFrame(new_row, index=[0]), schema=self.log_table.schema
        )

        with self.write_lock:
            self.log_table = pa.concat_tables([self.log_table, new_row])

    def executeTrigger(
        self,
        trigger: TriggerFn,
        triggered_by: TriggerElement,
    ):
        """Execute a trigger.

        Args:
            trigger (TriggerFn): The trigger to execute.
            triggered_by (TriggerElement): The element that triggered the trigger.
        """
        trigger_name, trigger_fn, isTransform = trigger
        logger.info(
            f"Running trigger {trigger_name} for identifier {triggered_by.identifier}, key {triggered_by.key}..."
        )
        new_connection = Connection(
            self.name,
            self.namespaces,
            self.log_table,
            self.table_columns,
            self.triggers,
            self.write_lock,
            self.session_id,
            self.wait_for_results,
        )

        if not isTransform:
            trigger_fn(
                new_connection,
                triggered_by,
            )
            # Log the trigger execution
            self.logTriggerExecution(trigger_name, 0, "function", triggered_by)

            logger.info(
                f"Finished running trigger {trigger_name} for identifier {triggered_by.identifier}."
            )

        else:
            # Get route for key
            route = trigger_fn.route_map.get(
                f"{triggered_by.namespace}.{triggered_by.key}", None
            )
            if route is None:
                raise ValueError(
                    f"Route not found for {triggered_by.namespace}.{triggered_by.key}."
                )

            # Execute the transform lifecycle: infer -> fit
            if route.infer is not None:
                route.infer(new_connection, triggered_by)
                self.logTriggerExecution(
                    trigger_name, trigger_fn.version, "infer", triggered_by
                )

            # Fit is asynchronous
            if route.fit is not None:
                fit_thread = trigger_fn.fitWrapper(
                    new_connection,
                    trigger_name,
                    triggered_by,
                )
                self.fit_events.append(fit_thread)
            else:
                logger.info(
                    f"Finished running trigger {trigger_name} for identifier {triggered_by.identifier}."
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
            # self.cur.execute(
            #     f"INSERT INTO {self.name}.{namespace} SELECT '{new_id}' AS identifier, '{identifier}' AS derived_id, {', '.join(self.table_columns[namespace])} FROM {self.name}.{namespace} WHERE identifier = '{identifier}'"
            # )
            table = self.namespaces[namespace]
            condition = pc.equal(table["identifier"], identifier)

            row = pc.filter(table, condition).to_pandas()
            row.at[0, "identifier"] = new_id
            row.at[0, "derived_id"] = identifier

            new_row = pa.Table.from_pandas(row, schema=table.schema)

            # filtered_table = pc.filter(table, pc.invert(condition))
            final_table = pa.concat_tables([table, new_row])
            self.namespaces[namespace] = final_table

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
            include_derived (bool, optional): Whether to include derived ids. Defaults to False.
            filter_null (bool, optional): Whether to filter out null values. Only used in conjuction with include_derived. Defaults to True.

        Returns:
            typing.Any: The values for the keys.
        """

        con = duckdb.connect()
        scanner = pa.dataset.Scanner.from_dataset(
            pa.dataset.dataset(self.namespaces[namespace])
        )

        if not kwargs.get("include_derived", False):
            res = con.execute(
                f"SELECT {', '.join(keys)} FROM scanner WHERE identifier = '{identifier}'"
            ).fetchone()
            res_dict = {k: v for k, v in zip(keys, res)}
            res_dict.update({"identifier": identifier})

            return (
                pd.Series(res_dict).to_frame().T
                if kwargs.get("as_df", False)
                else res_dict
            )

        # Recursively get derived ids
        id_res = con.execute(
            f"SELECT identifier FROM scanner WHERE derived_id = '{identifier}'"
        ).fetchall()
        id_res = [i[0] for i in id_res]
        all_ids = [identifier] + id_res
        while len(id_res) > 0:
            id_res_str = [f"'{str(i)}'" for i in id_res]
            id_res = con.execute(
                f"SELECT identifier FROM scanner WHERE derived_id IN ({', '.join(id_res_str)})"
            ).fetchall()
            id_res = [i[0] for i in id_res]
            all_ids.extend(id_res)

        if "identifier" not in keys:
            keys.append("identifier")

        return self.mget(namespace, all_ids, keys, **kwargs)

    def mget(
        self,
        namespace: str,
        identifiers: typing.List[str],
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


        Returns:
            pd.DataFrame: The values for the key.
        """

        con = duckdb.connect()
        scanner = pa.dataset.Scanner.from_dataset(
            pa.dataset.dataset(self.namespaces[namespace])
        )

        all_ids_str = [f"'{str(i)}'" for i in identifiers]
        if "identifier" not in keys:
            keys.append("identifier")
        res = con.execute(
            f"SELECT {', '.join(keys)} FROM scanner WHERE identifier IN ({', '.join(all_ids_str)})"
        ).fetch_arrow_table()

        if kwargs.get("filter_null", True):
            res = pa.compute.drop_null(res).combine_chunks()

        res = res.to_pandas()
        if kwargs.get("as_df", False):
            return res

        return res.to_dict("records")

    def getIdsForKey(
        self, namespace: str, key: str, value: typing.Any
    ) -> typing.List[int]:
        """Get ids for a key-value pair in a namespace.

        Args:
            namespace (str): The namespace to get the value from.
            key (str): The key to get the values for.
            value (typing.Any): The value to get the ids for.

        Returns:
            typing.List[int]: The ids for the key-value pair.
        """
        con = duckdb.connect()
        scanner = pa.dataset.Scanner.from_dataset(
            pa.dataset.dataset(self.namespaces[namespace])
        )
        # table = self.namespaces[namespace]

        res = con.execute(
            f"SELECT identifier FROM scanner WHERE {key} = ?",
            (value,),
        ).fetchall()
        return [r[0] for r in res]

    def sql(self, stmt: str, as_df: bool = True) -> typing.Any:
        con = duckdb.connect()

        # Create a table for each namespace
        for namespace, table in self.namespaces.items():
            locals()[namespace] = table

        return (
            con.execute(stmt).fetch_arrow_table().to_pandas()
            if as_df
            else con.execute(stmt).fetchall()
        )
