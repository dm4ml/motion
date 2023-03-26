"""
Database connection, with functions that a users is allowed to
call within trigger lifecycle methods.
"""
import collections
import duckdb
import itertools
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
from motion.utils import logger, TriggerElement, TriggerFn


class Cursor(object):
    def __init__(
        self,
        *,
        name: str,
        relations: typing.Dict[str, pa.Table],
        log_table: pa.Table,
        table_columns: typing.Dict[str, typing.List[str]],
        triggers: typing.Dict[str, typing.List[TriggerFn]],
        write_lock: threading.Lock,
        session_id: str,
        wait_for_results: bool = False,
        triggers_to_run_on_duplicate: typing.Dict[
            TriggerFn, TriggerElement
        ] = {},
        spawned_by: TriggerElement = None,  # type: ignore
    ):
        self.name = name
        self.relations: typing.Dict[str, pa.Table] = relations
        self.log_table = log_table
        self.table_columns = table_columns
        self.triggers = triggers
        self.write_lock = write_lock
        self.session_id = session_id
        self.wait_for_results = wait_for_results
        self.fit_events: typing.List[threading.Event] = []
        self.triggers_to_run_on_duplicate = triggers_to_run_on_duplicate
        self.spawned_by = spawned_by

    def __del__(self) -> None:
        if self.wait_for_results:
            self.waitForResults()

    def waitForResults(self) -> None:
        for t in self.fit_events:
            t.wait()
        self.fit_events = []

    def getNewId(self, relation: str, key: str = "identifier") -> str:
        """Get a new id for a relation.

        Args:
            relation (str): The relation to get the new id for.
            key (str, optional): The key to get the new id for. Defaults to "id".

        Returns:
            str: The new id.
        """
        new_id = str(uuid.uuid4())

        # Check if the id already exists
        if self.exists(relation, new_id):
            return self.getNewId(relation, key)

        return new_id

    def exists(
        self,
        relation: str,
        identifier: str,
    ) -> bool:
        """Determine if a record exists in a relation.

        Args:
            relation (str): The relation to check.
            identifier (str): The primary key of the record.

        Returns:
            bool: True if the record exists, False otherwise.
        """
        if relation not in self.relations.keys():
            raise KeyError(f"relation {relation} does not exist.")

        # Check if identifier exists in pyarrow table
        table = self.relations[relation]
        condition = pc.equal(table["identifier"], identifier)
        mask = pc.filter(table["identifier"], condition)
        result = len(mask) > 0

        return result

    def set(
        self,
        *,
        relation: str,
        identifier: str,
        key_values: typing.Dict[str, typing.Any],
    ) -> str:
        """Set multiple values for a key in a relation.
        TODO(shreyashankar): Handle complex types.

        Args:
            relation (str): The relation to set the value in.
            identifier (str): The id of the record to set the value for.
            key_values (typing.Dict[str, typing.Any]): The key-value pairs to set.

        Returns:
            str: The identifier of the record.
        """
        if relation is None:
            raise ValueError("relation cannot be None.")

        if relation not in self.relations:
            raise KeyError(f"relation {relation} does not exist.")

        exists = True
        if not identifier:
            identifier = self.getNewId(relation)
            exists = False

        if exists:
            exists = self.exists(relation, identifier)

        # Convert enums to their values
        for key, value in key_values.items():
            if isinstance(value, Enum):
                key_values.update({key: value.value})

        # Insert or update based on identifier
        with self.write_lock:
            table = self.relations[relation]

            if not exists:
                new_row_dict = {n: None for n in table.schema.names}
                new_row_dict.update(
                    {
                        "identifier": identifier,  # type: ignore
                        "create_at": pd.Timestamp.now(),  # type: ignore
                        "session_id": self.session_id,  # type: ignore
                    }
                )
                new_row_dict.update(key_values)
                new_row_df = pd.DataFrame(new_row_dict, index=[0])
                new_row = pa.Table.from_pandas(new_row_df, schema=table.schema)

                # Check schemas match
                if collections.Counter(
                    new_row.schema.names
                ) != collections.Counter(new_row_df.columns.values):
                    raise AttributeError(
                        f"One of the keys you are trying to set is not a valid key in the relation {relation}. Please double check your keys."
                    )

                final_table = pa.concat_tables([table, new_row])
                self.relations[relation] = final_table

            else:
                condition = pc.equal(table["identifier"], identifier)
                row = pc.filter(table, condition).to_pandas()

                for key, value in key_values.items():
                    row.at[0, key] = value

                new_row = pa.Table.from_pandas(row, schema=table.schema)

                filtered_table = pc.filter(table, pc.invert(condition))
                final_table = pa.concat_tables([filtered_table, new_row])
                self.relations[relation] = final_table

        # Get all triggers to run
        triggers_to_run: typing.Dict[TriggerFn, TriggerElement] = {}
        for key, value in key_values.items():
            if f"{relation}.{key}" not in self.triggers:
                continue

            triggered_by = TriggerElement(
                relation=relation,
                identifier=identifier,
                key=key,
                value=value,
            )
            for trigger in self.triggers.get(f"{relation}.{key}", []):
                if trigger in triggers_to_run.keys():
                    continue

                triggers_to_run[trigger] = triggered_by

        # Run triggers, passing in remainder of triggers_to_run
        for trigger, triggered_by in triggers_to_run.items():
            other_triggers_to_run = {
                k: triggers_to_run[k] for k in triggers_to_run if k != trigger
            }
            self.executeTrigger(
                trigger=trigger,
                triggered_by=triggered_by,
                triggers_to_run_on_duplicate=other_triggers_to_run,
            )

        return identifier

    def logTriggerExecution(
        self,
        trigger_name: str,
        trigger_version: int,
        trigger_action: str,
        triggered_by: TriggerElement,
    ) -> None:
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
            "relation": triggered_by.relation,
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
        *,
        trigger: TriggerFn,
        triggered_by: TriggerElement,
        triggers_to_run_on_duplicate: typing.Dict[
            TriggerFn, TriggerElement
        ] = {},
    ) -> None:
        """Execute a trigger.

        Args:
            trigger (TriggerFn): The trigger to execute.
            triggered_by (TriggerElement): The element that triggered the trigger.
            triggers_to_run (typing.Dict[TriggerFn, TriggerElement], optional): The triggers to run whenever duplicate is called within a trigger. Defaults to {}.
        """
        try:
            self._executeTrigger(
                trigger, triggered_by, triggers_to_run_on_duplicate
            )
        except RecursionError:
            raise RecursionError(
                f"Recursion error in trigger {trigger[0]}. Please make sure you do not have a cycle in your triggers."
            )

    def _executeTrigger(
        self,
        trigger: TriggerFn,
        triggered_by: TriggerElement,
        triggers_to_run_on_duplicate: typing.Dict[
            TriggerFn, TriggerElement
        ] = {},
    ) -> None:
        """Execute a trigger.

        Args:
            trigger (TriggerFn): The trigger to execute.
            triggered_by (TriggerElement): The element that triggered the trigger.
            triggers_to_run (typing.Dict[TriggerFn, TriggerElement], optional): The triggers to run whenever duplicate is called within a trigger. Defaults to {}.
        """
        trigger_name, trigger_fn, isTransform = trigger
        logger.info(
            f"Running trigger {trigger_name} for identifier {triggered_by.identifier}, key {triggered_by.key}..."
        )
        new_connection = Cursor(
            name=self.name,
            relations=self.relations,
            log_table=self.log_table,
            table_columns=self.table_columns,
            triggers=self.triggers,
            write_lock=self.write_lock,
            session_id=self.session_id,
            wait_for_results=self.wait_for_results,
            triggers_to_run_on_duplicate=triggers_to_run_on_duplicate,
            spawned_by=triggered_by,
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
                f"{triggered_by.relation}.{triggered_by.key}", None
            )
            if route is None:
                raise NotImplementedError(
                    f"Route not found for {triggered_by.relation}.{triggered_by.key}."
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

    def duplicate(self, *, relation: str, identifier: str) -> str:
        """Duplicate a record in a relation. Doesn't run triggers.

        Args:
            relation (str): The relation to duplicate the record in.
            identifier (str): The identifier of the record to duplicate.

        Returns:
            str: The new identifier of the duplicated record.
        """
        new_id = self.getNewId(relation)

        with self.write_lock:
            # self.cur.execute(
            #     f"INSERT INTO {self.name}.{relation} SELECT '{new_id}' AS identifier, '{identifier}' AS derived_id, {', '.join(self.table_columns[relation])} FROM {self.name}.{relation} WHERE identifier = '{identifier}'"
            # )
            table = self.relations[relation]
            condition = pc.equal(table["identifier"], identifier)

            row = pc.filter(table, condition).to_pandas()
            row.at[0, "identifier"] = new_id
            row.at[0, "derived_id"] = identifier

            new_row = pa.Table.from_pandas(row, schema=table.schema)

            # filtered_table = pc.filter(table, pc.invert(condition))
            final_table = pa.concat_tables([table, new_row])
            self.relations[relation] = final_table

        # Run triggers on duplicate if it was from element that spawned
        # the trigger in the first place
        if (
            self.spawned_by is not None
            and self.spawned_by.identifier == identifier
            and self.spawned_by.relation == relation
        ):
            for (
                trigger,
                triggered_by,
            ) in self.triggers_to_run_on_duplicate.items():
                other_triggers_to_run = {
                    k: self.triggers_to_run_on_duplicate[k]
                    for k in self.triggers_to_run_on_duplicate
                    if k != trigger
                }
                new_triggered_by = triggered_by._replace(identifier=new_id)
                self.executeTrigger(
                    trigger=trigger,
                    triggered_by=new_triggered_by,
                    triggers_to_run_on_duplicate=other_triggers_to_run,
                )

        return new_id

    def get(
        self,
        *,
        relation: str,
        identifier: str,
        keys: typing.List[str],
        **kwargs: typing.Any,
    ) -> typing.Any:
        """Get values for an identifier's keys in a relation.
        TODO: Handle complex types.

        Args:
            relation (str): The relation to get the value from.
            identifier (str): The identifier of the record to get the value for.
            keys (typing.List[str]): The keys to get the values for.

        Keyword Args:
            include_derived (bool, optional): Whether to include derived ids. Defaults to False.
            filter_null (bool, optional): Whether to filter out null values. Only used in conjuction with include_derived. Defaults to True.

        Returns:
            typing.Any: The values for the keys.
        """

        if keys == ["*"]:
            keys = self.relations[relation].schema.names

        if not keys or not all(
            [k in self.relations[relation].schema.names for k in keys]
        ):
            raise ValueError(
                f"Not all keys {keys} not found in relation {relation}."
            )

        con = duckdb.connect()
        scanner = pa.dataset.Scanner.from_dataset(
            pa.dataset.dataset(self.relations[relation])
        )

        if not kwargs.get("include_derived", False):
            res = con.execute(
                f"SELECT {', '.join(keys)} FROM scanner WHERE identifier = '{identifier}'"
            ).fetchone()

            if not res:
                raise ValueError(
                    f"Identifier {identifier} not found in relation {relation}."
                )

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

        return self.mget(
            relation=relation,
            identifiers=all_ids,
            keys=keys,
            compute_derived=False,
            **kwargs,
        )

    def _get_derived_ids(
        self, con: duckdb.DuckDBPyConnection, identifier: str
    ) -> typing.List[str]:
        """Get all derived ids for an identifier.

        Args:
            con (duckdb.PyConnection): The connection to use.
            identifier (str): The identifier to get the derived ids for.

        Returns:
            typing.List[str]: The derived ids.
        """
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
        return all_ids

    def mget(
        self,
        *,
        relation: str,
        identifiers: typing.List[str],
        keys: typing.List[str],
        **kwargs: typing.Any,
    ) -> pd.DataFrame:
        """Get multiple values for keys in a relation.
        TODO: Handle complex types.

        Args:
            relation (str): The relation to get the value from.
            identifiers (typing.List[int]): The ids of the records to get the value for.
            keys (typing.List[str]): The keys to get the values for.
            filter_null (bool, optional): Whether to filter out null values.  Defaults to True.


        Returns:
            pd.DataFrame: The values for the key.
        """

        con = duckdb.connect()
        scanner = pa.dataset.Scanner.from_dataset(
            pa.dataset.dataset(self.relations[relation])
        )

        # Find all derived ids
        if kwargs.get("compute_derived", True):
            all_derived_ids = []
            for identifier in identifiers:
                if not self.exists(relation, identifier):
                    raise ValueError(
                        f"Identifier {identifier} not found in relation {relation}."
                    )
                all_derived_ids.extend(self._get_derived_ids(con, identifier))
            identifiers = list(set(identifiers + all_derived_ids))

        all_ids_str = [f"'{str(i)}'" for i in identifiers]

        if keys == ["*"]:
            keys = self.relations[relation].schema.names

        if "identifier" not in keys:
            keys.append("identifier")
        res = con.execute(
            f"SELECT {', '.join(keys)} FROM scanner WHERE identifier IN ({', '.join(all_ids_str)})"
        ).fetch_arrow_table()

        if kwargs.get("filter_null", True):
            if "derived_id" not in keys:
                res = pa.compute.drop_null(res).combine_chunks()

            else:
                # Filter out rows where all columns except derived_id are null
                keep_null_idx = res.schema.get_field_index("derived_id")
                col_indices = [
                    i
                    for i in range(len(res.schema.names))
                    if i != keep_null_idx
                ]
                valid_cols = [
                    pa.compute.is_valid(res.column(i)) for i in col_indices
                ]
                cond = valid_cols[0]
                for i in range(1, len(valid_cols)):
                    cond = pa.compute.and_(cond, valid_cols[i])
                res = res.filter(cond)

        res = res.to_pandas()
        if kwargs.get("as_df", False):
            return res

        return res.to_dict("records")

    def getIdsForKey(
        self, relation: str, key: str, value: typing.Any
    ) -> typing.List[int]:
        """Get ids for a key-value pair in a relation.

        Args:
            relation (str): The relation to get the value from.
            key (str): The key to get the values for.
            value (typing.Any): The value to get the ids for.

        Returns:
            typing.List[int]: The ids for the key-value pair.
        """
        con = duckdb.connect()
        scanner = pa.dataset.Scanner.from_dataset(
            pa.dataset.dataset(self.relations[relation])
        )
        # table = self.relations[relation]

        res = con.execute(
            f"SELECT identifier FROM scanner WHERE {key} = ?",
            (value,),
        ).fetchall()
        return [r[0] for r in res]

    def sql(self, stmt: str, as_df: bool = True) -> typing.Any:
        con = duckdb.connect()

        # Create a table for each relation
        for relation, table in self.relations.items():
            locals()[relation] = table

        return (
            con.execute(stmt).fetch_arrow_table().to_pandas()
            if as_df
            else con.execute(stmt).fetchall()
        )
