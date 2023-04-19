"""
Database connection, with functions that a users is allowed to
call within trigger lifecycle methods.
"""


import collections
import threading
import typing
import uuid
from enum import Enum
from typing import Any

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from motion.utils import TriggerElement, TriggerFn, logger


class Cursor:
    """A connection to a Motion data store, only accessible within Motion triggers."""

    def __init__(
        self,
        *,
        name: str,
        relations: typing.Dict[str, pa.Table],
        log_table: pa.Table,
        table_columns: typing.Dict[str, list[str]],
        triggers: typing.Dict[str, list[TriggerFn]],
        write_lock: threading.Lock,
        session_id: str,
        wait_for_results: bool = False,
        triggers_to_run_on_duplicate: typing.Dict[TriggerFn, TriggerElement] = {},
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
        self.fit_events: list[threading.Event] = []
        self.triggers_to_run_on_duplicate = triggers_to_run_on_duplicate
        self.spawned_by = spawned_by

    def __del__(self) -> None:
        if self.wait_for_results:
            self.waitForResults()

    def close(self) -> None:
        """Closes the cursor."""
        self.__del__()

    def waitForResults(self) -> None:
        """Waits for all fit events to finish."""
        for t in self.fit_events:
            t.wait()
        self.fit_events = []

    def getNewId(self, relation: str, key: str = "identifier") -> str:
        """Gets a new id for a relation.

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
        """Determines if a record exists in a relation.

        Args:
            relation (str): The relation to check.
            identifier (str): The record's identifier.

        Returns:
            bool: True if the record exists, False otherwise.
        """
        if relation not in self.relations.keys():
            raise KeyError(f"Relation {relation} does not exist.")

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
        key_values: typing.Dict[str, typing.Any],
        identifier: str = "",
    ) -> str:
        """Sets given key-value pairs for an identifier in a relation.
        Overwrites existing values.

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
            raise KeyError(f"Relation {relation} does not exist.")

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
                new_row_df = pd.DataFrame([new_row_dict])

                try:
                    new_row = pa.Table.from_pandas(new_row_df, schema=table.schema)
                except pa.ArrowInvalid as e:
                    raise TypeError(
                        f"Invalid key-value pair for relation {relation}. Make sure the values are of the correct type. Full error: {e}"
                    )
                except pa.ArrowTypeError as e:
                    raise TypeError(
                        f"Invalid key-value pair for relation {relation}. Make sure the values are of the correct type. Full error: {e}"
                    )

                # Check schemas match
                if collections.Counter(new_row.schema.names) != collections.Counter(
                    new_row_df.columns.values
                ):
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

            trigger_context = TriggerElement(
                relation=relation,
                identifier=identifier,
                key=key,
                value=value,
            )
            for trigger in self.triggers.get(f"{relation}.{key}", []):
                if trigger in triggers_to_run.keys():
                    continue

                triggers_to_run[trigger] = trigger_context

        # Run triggers, passing in remainder of triggers_to_run
        for trigger, trigger_context in triggers_to_run.items():
            other_triggers_to_run = {
                k: triggers_to_run[k] for k in triggers_to_run if k != trigger
            }
            self.executeTrigger(
                trigger=trigger,
                trigger_context=trigger_context,
                triggers_to_run_on_duplicate=other_triggers_to_run,
            )

        return identifier

    def logTriggerExecution(
        self,
        trigger_name: str,
        trigger_version: int,
        trigger_action: str,
        trigger_action_type: str,
        trigger_context: TriggerElement,
    ) -> None:
        """Logs the execution of a trigger.

        Args:
            trigger_name (str): The name of the trigger.
            trigger_version (int): The version of the trigger.
            trigger_action (str): The action of the trigger (method name).
            trigger_action_type (str): The type of action (infer or fit).
            trigger_context (TriggerElement): The element that triggered the trigger.
        """

        # Append to the log table
        new_row = {
            "executed_time": pd.Timestamp.now(),
            "session_id": self.session_id,
            "trigger_name": trigger_name,
            "trigger_version": trigger_version,
            "trigger_action": trigger_action,
            "trigger_action_type": trigger_action_type,
            "relation": trigger_context.relation,
            "identifier": trigger_context.identifier,
            "trigger_key": trigger_context.key,
        }
        with self.write_lock:
            self.log_table.loc[len(self.log_table)] = pd.Series(new_row)

    def executeTrigger(
        self,
        *,
        trigger: TriggerFn,
        trigger_context: TriggerElement,
        triggers_to_run_on_duplicate: typing.Dict[TriggerFn, TriggerElement] = {},
    ) -> None:
        """Executes a trigger, logging completion of infer and fit methods.

        Args:
            trigger (TriggerFn): The trigger to execute.
            trigger_context (TriggerElement): The element that triggered the trigger.
            triggers_to_run (typing.Dict[TriggerFn, TriggerElement], optional): The triggers to run whenever duplicate is called within a trigger. Defaults to {}.
        """
        try:
            self._executeTrigger(trigger, trigger_context, triggers_to_run_on_duplicate)
        except RecursionError:
            raise RecursionError(
                f"Recursion error in trigger {trigger[0]}. Please make sure you do not have a cycle in your triggers."
            )

    def _executeTrigger(
        self,
        trigger: TriggerFn,
        trigger_context: TriggerElement,
        triggers_to_run_on_duplicate: typing.Dict[TriggerFn, TriggerElement] = {},
    ) -> None:
        """Execute a trigger.

        Args:
            trigger (TriggerFn): The trigger to execute.
            trigger_context (TriggerElement): The element that triggered the trigger.
            triggers_to_run (typing.Dict[TriggerFn, TriggerElement], optional): The triggers to run whenever duplicate is called within a trigger. Defaults to {}.
        """
        trigger_name, trigger_fn = trigger
        logger.info(
            f"Running trigger {trigger_name} for identifier {trigger_context.identifier}, key {trigger_context.key}..."
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
            spawned_by=trigger_context,
        )

        # Get route for key
        route = trigger_fn.route_map.get(
            f"{trigger_context.relation}.{trigger_context.key}", None
        )
        if route is None:
            raise NotImplementedError(
                f"Route not found for {trigger_context.relation}.{trigger_context.key}."
            )

        # Execute the transform lifecycle: infer -> fit
        infer_context = None
        if route.infer is not None:
            infer_context = route.infer(new_connection, trigger_context)
            self.logTriggerExecution(
                trigger_name,
                trigger_fn.version,
                route.infer.__name__,
                "INFER",
                trigger_context,
            )

        # Fit is asynchronous
        if route.fit is not None:
            fit_thread = trigger_fn.fitWrapper(
                new_connection, trigger_name, trigger_context, infer_context
            )
            self.fit_events.append(fit_thread)
        else:
            logger.info(
                f"Finished running trigger {trigger_name} for identifier {trigger_context.identifier}."
            )

    def duplicate(self, relation: str, identifier: str) -> str:
        """Duplicates a record in a relation. Doesn't rerun any triggers for old keys on the new record.

        Args:
            relation (str): The relation to duplicate the record in.
            identifier (str): The identifier of the record to duplicate.

        Returns:
            str: The new identifier of the duplicated record.
        """
        new_id = self.getNewId(relation)

        with self.write_lock:
            table = self.relations[relation]
            condition = pc.equal(table["identifier"], identifier)

            row = pc.filter(table, condition).to_pandas()
            row.at[0, "identifier"] = new_id
            row.at[0, "derived_id"] = identifier

            new_row = pa.Table.from_pandas(row, schema=table.schema)

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
                trigger_context,
            ) in self.triggers_to_run_on_duplicate.items():
                other_triggers_to_run = {
                    k: self.triggers_to_run_on_duplicate[k]
                    for k in self.triggers_to_run_on_duplicate
                    if k != trigger
                }
                new_trigger_context = trigger_context._replace(identifier=new_id)
                self.executeTrigger(
                    trigger=trigger,
                    trigger_context=new_trigger_context,
                    triggers_to_run_on_duplicate=other_triggers_to_run,
                )

        return new_id

    def get(
        self,
        *,
        relation: str,
        identifier: str,
        keys: list[str],
        **kwargs: typing.Any,
    ) -> typing.Any:
        """Get values for an identifier's keys in a relation. Can pass in ["*"] as the keys argument to get all keys.

        Args:
            relation (str): The relation to get the value from.
            identifier (str): The identifier of the record to get the value for.
            keys (typing.List[str]): The keys to get the values for.

        Keyword Args:
            include_derived (bool, optional): Whether to include derived ids in the result. Defaults to False.
            filter_null (bool, optional): Whether to filter out null values. Filters all records with any null walue for any of the keys requested. Only used in conjuction with include_derived. Defaults to True.
            as_df (bool, optional): Whether to return the result as a pandas dataframe. Defaults to False.

        Returns:
            typing.Any: The values for the keys.
        """

        if not identifier:
            raise ValueError("Identifier cannot be empty.")

        if keys == ["*"]:
            keys = self.relations[relation].schema.names

        if not keys or not all(
            [k in self.relations[relation].schema.names for k in keys]
        ):
            raise ValueError(f"Not all keys {keys} not found in relation {relation}.")

        con = duckdb.connect()
        scanner = pa.dataset.Scanner.from_dataset(
            pa.dataset.dataset(self.relations[relation])
        )

        if not kwargs.get("include_derived", False):
            res = con.execute(
                f"SELECT {', '.join(keys)} FROM scanner WHERE identifier = '{identifier}'"
            ).fetchall()

            if not res:
                return res

            res = res[0]
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

        kwargs.update({"include_derived": False})
        return self.mget(
            relation=relation,
            identifiers=all_ids,
            keys=keys,
            **kwargs,
        )

    def _get_derived_ids(self, con: duckdb.DuckDBPyConnection, identifier: str) -> Any:
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
        identifiers: list[str],
        keys: list[str],
        **kwargs: typing.Any,
    ) -> pd.DataFrame:
        """Get values for a many identifiers' keys in a relation. Can pass in ["*"] as the keys argument to get all keys.

        Args:
            relation (str): The relation to get the value from.
            identifiers (typing.List[int]): The ids of the records to get the value for.
            keys (typing.List[str]): The keys to get the values for.

        Keyword Args:
            include_derived (bool, optional): Whether to include derived ids in the result. Defaults to False.
            filter_null (bool, optional): Whether to filter out null values. Filters all records with any null walue for any of the keys requested. Only used in conjuction with include_derived. Defaults to True.
            as_df (bool, optional): Whether to return the result as a pandas dataframe. Defaults to False.


        Returns:
            pd.DataFrame: The values for the key.
        """

        con = duckdb.connect()
        scanner = pa.dataset.Scanner.from_dataset(
            pa.dataset.dataset(self.relations[relation])
        )

        # Find all derived ids
        if kwargs.get("include_derived", False):
            all_derived_ids = []
            for identifier in identifiers:
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
                    i for i in range(len(res.schema.names)) if i != keep_null_idx
                ]
                valid_cols = [pa.compute.is_valid(res.column(i)) for i in col_indices]
                cond = valid_cols[0]
                for i in range(1, len(valid_cols)):
                    cond = pa.compute.and_(cond, valid_cols[i])
                res = res.filter(cond)

        res = res.to_pandas()
        if kwargs.get("as_df", False):
            return res

        return res.to_dict("records")

    def getIdsForKey(self, relation: str, key: str, value: Any) -> Any:
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

    def sql(self, query: str, as_df: bool = True) -> typing.Any:
        """Executes a SQL query on the relations. Specify the relations as tables in the query.

        Args:
            query (str): SQL query to execute.
            as_df (bool, optional): Whether to return the result as a pandas dataframe. Defaults to True.

        Returns:
            typing.Any: Pandas dataframe if as_df is True, else list of tuples.
        """
        con = duckdb.connect()

        # Create a table for each relation
        for relation, table in self.relations.items():
            locals()[relation] = table

        return (
            con.execute(query).fetch_arrow_table().to_pandas()
            if as_df
            else con.execute(query).fetchall()
        )
