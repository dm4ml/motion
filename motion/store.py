import dill
import duckdb
import inspect
import logging
import os
import pandas as pd
import typing

from collections import namedtuple
from enum import Enum
from motion import Transform, Schema
from motion.transform import TriggerElement

CONNECTIONS = {}


def get_store(name: str, create: bool, memory: bool = True) -> typing.Any:
    """Get or create a store with the given name.

    Args:
        name (str): The name of the store to get or create.
        create (bool): Whether to create the store if it doesn't exist.
        memory (bool, optional): Whether to use memory for the store. Defaults to True.

    Returns:
        typing.Any: The store.
    """
    if name not in CONNECTIONS:
        if not create:
            raise Exception(f"Store {name} does not exist. Set create=True.")

        CONNECTIONS[name] = Store(name, memory=memory)

    return CONNECTIONS[name]


TriggerFn = namedtuple("TriggerFn", ["name", "fn", "isTransform"])


class Store(object):
    def __init__(self, name: str, memory: bool = True):
        self.name = name
        self.memory = memory

        if not memory and not os.path.exists(f"datastores/{name}"):
            os.makedirs(f"datastores/{name}")

        self.con = (
            duckdb.connect(":memory:")
            if self.memory
            else duckdb.connect(f"datastores/{name}/duck.db")
        )
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {name}")
        self.triggers = (
            dill.load(open(f"datastores/{name}/triggers", "rb"))
            if os.path.exists(f"datastores/{name}/triggers")
            else {}
        )
        self.trigger_names = (
            dill.load(open(f"datastores/{name}/trigger_names", "rb"))
            if os.path.exists(f"datastores/{name}/trigger_names")
            else {}
        )
        self.trigger_fns = (
            dill.load(open(f"datastores/{name}/trigger_fns", "rb"))
            if os.path.exists(f"datastores/{name}/trigger_fns")
            else {}
        )

        self.table_columns = (
            dill.load(open(f"datastores/{name}/table_columns", "rb"))
            if os.path.exists(f"datastores/{name}/table_columns")
            else {}
        )

    # def __del__(self):
    #     # Close connection and persist triggers
    #     self.con.close()

    #     if not self.memory:
    #         dill.dump(
    #             self.triggers, open(f"datastores/{self.name}/triggers", "wb")
    #         )
    #         dill.dump(
    #             self.trigger_names,
    #             open(f"datastores/{self.name}/trigger_names", "wb"),
    #         )
    #         dill.dump(
    #             self.trigger_fns,
    #             open(f"datastores/{self.name}/trigger_fns", "wb"),
    #         )
    #         dill.dump(
    #             self.table_columns,
    #             open(f"datastores/{self.name}/table_columns", "wb"),
    #         )

    def addNamespace(self, name: str, schema: typing.Any) -> None:
        """Add a namespace to the store.

        Args:
            name (str): The name of the namespace.
            schema (typing.Any): The schema of the namespace.
        """

        # Check if namespace already exists
        tables = self.con.execute(f"SHOW TABLES;").fetchall()
        tables = [t[0] for t in tables]
        if name in tables:
            logging.warning(
                f"Namespace {name} already exists in store {self.name}. Doing nothing."
            )
            return

        stmts = schema.formatCreateStmts(f"{self.name}.{name}")
        for stmt in stmts:
            logging.info(stmt)
            self.con.execute(stmt)

        # Create sequence for id
        self.con.execute(f"CREATE SEQUENCE {self.name}.{name}_id_seq;")

        # Store column names
        self.table_columns[name] = (
            self.con.execute(f"DESCRIBE {self.name}.{name};")
            .fetchdf()["column_name"]
            .tolist()
        )
        self.table_columns[name].remove("id")
        self.table_columns[name].remove("derived_id")

        # Persist
        if not self.memory:
            dill.dump(
                self.table_columns,
                open(f"datastores/{self.name}/table_columns", "wb"),
            )

    def deleteNamespace(self, name: str) -> None:
        """Delete a namespace from the store.
        TODO(shreya): Error checking

        Args:
            name (str): The name of the namespace.
        """
        self.con.execute(f"DROP TABLE {self.name}.{name};")
        self.con.execute(f"DROP SEQUENCE {self.name}.{name}_id_seq;")

        # Persist
        if not self.memory:
            dill.dump(
                self.table_columns,
                open(f"datastores/{self.name}/table_columns", "wb"),
            )

    def addTrigger(
        self,
        name: str,
        keys: typing.List[str],
        trigger: typing.Union[typing.Callable, type],
    ) -> None:
        """Adds a trigger to the store.

        Args:
            name (str): Trigger name.
            keys (typing.List[str]): Names of the keys to triger on. Formatted as "namespace.key". Trigger executes if there is a addition to any of the keys.
            trigger (typing.Union[typing.Callable, type]): Function or class to execute when the trigger is fired. If function, must take in the id of the row that triggered the trigger, a reference to the element that triggered it, and a reference to the store object (in this order). If class, must implement the Transform interface.

        Raises:
            ValueError: If there is already a trigger with the given name.
        """
        if name in self.trigger_names:
            logging.warning(f"Trigger {name} already exists. Doing nothing.")
            return

        if inspect.isfunction(trigger):
            # Check that the function signature is correct
            if len(inspect.signature(trigger).parameters) != 3:
                raise ValueError(
                    f"Trigger function must take in 2 arguments: id, triggered_by, and store."
                )

        elif inspect.isclass(trigger):
            # Check that the class implements the Transform interface
            if not issubclass(trigger, Transform):
                raise ValueError(
                    f"Trigger class must implement the Transform interface."
                )

        else:
            raise ValueError(
                f"Trigger {name} must be a function or class. Got {type(trigger)}."
            )

        # Add the trigger to the store
        self.trigger_names[name] = keys
        self.trigger_fns[name] = trigger(self)
        trigger_exec = trigger(self) if inspect.isclass(trigger) else trigger
        for key in keys:
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
            self.triggers[key].remove((name, fn))
        del self.trigger_names[name]
        del self.trigger_fns[name]

    def getTriggersForKey(self, key: str) -> typing.List[str]:
        """Get the list of triggers for a given key.

        Args:
            key (str): The key to get the triggers for.

        Returns:
            typing.List[str]: The list of triggers for the given key.
        """
        names_and_fns = self.triggers.get(key, [])
        return [t[0] for t in names_and_fns]

    def getTriggersForAllKeys(self) -> typing.Dict[str, typing.List[str]]:
        """Get the list of triggers for all keys.

        Returns:
            typing.Dict[str, typing.List[str]]: The list of triggers for all keys.
        """
        return {k: self.getTriggersForKey(k) for k in self.triggers.keys()}

    def getNewId(self, namespace: str, key: str = "id") -> int:
        """Get a new id for a namespace.

        Args:
            namespace (str): The namespace to get the new id for.
            key (str, optional): The key to get the new id for. Defaults to "id".

        Returns:
            int: The new id.
        """

        self.con.execute(
            f"CREATE SEQUENCE IF NOT EXISTS {self.name}.{namespace}_{key}_seq;"
        )
        return self.con.execute(
            f"SELECT NEXTVAL('{self.name}.{namespace}_{key}_seq')"
        ).fetchone()[0]

    def exists(self, namespace: str, id: int) -> bool:
        """Determine if a record exists in a namespace.

        Args:
            namespace (str): The namespace to check.
            id (int): The primary key of the record.

        Returns:
            bool: True if the record exists, False otherwise.
        """
        elem = self.con.execute(
            f"SELECT id FROM {self.name}.{namespace} WHERE id = {id}"
        ).fetchone()
        return elem is not None

    def executeTrigger(
        self, id: int, trigger: TriggerFn, trigger_elem: TriggerElement
    ):
        """Execute a trigger.

        Args:
            id (int): The id of the record that triggered the trigger.
            trigger (TriggerFn): The trigger to execute.
            trigger_elem (TriggerElement): The element that triggered the trigger.
        """
        trigger_name, trigger_fn, isTransform = trigger
        logging.info(f"Running trigger {trigger_name} for {trigger_elem}...")
        if not isTransform:
            trigger_fn(id, trigger_elem, self)
        else:
            # Execute the transform lifecycle
            if trigger_fn.shouldFit(id, trigger_elem):
                trigger_fn.fit(id, trigger_elem)
            trigger_fn.transform(id, trigger_elem)
        logging.info(f"Finished running trigger {trigger_name}.")

    def set(
        self,
        namespace: str,
        id: int,
        key_values: typing.Dict[str, typing.Any],
        run_duplicates: bool = False,
    ) -> int:
        """Set multiple values for a key in a namespace.
        TODO(shreyashankar): Handle complex types.

        Args:
            namespace (str): The namespace to set the value in.
            id (int): The id of the record to set the value for.
            key_values (typing.Dict[str, typing.Any]): The key-value pairs to set.
            run_duplicates (bool, optional): Whether to run duplicate triggers. Defaults to False.
        """
        if not id:
            id = self.getNewId(namespace)

        # Convert enums to their values
        for key, value in key_values.items():
            if isinstance(value, Enum):
                key_values.update({key: value.value})

        if not self.exists(namespace, id):
            query_string = (
                f"INSERT INTO {self.name}.{namespace} (id, {', '.join(key_values.keys())}) VALUES (?, {', '.join(['?'] * len(key_values.keys()))})",
                (id, *key_values.values()),
            )
            self.con.execute(*query_string)

        else:
            # Delete and re-insert the row with the new value
            old_row = self.con.execute(
                f"SELECT * FROM {self.name}.{namespace} WHERE id = {id}"
            ).fetch_df()
            self.con.execute(
                f"DELETE FROM {self.name}.{namespace} WHERE id = ?;", (id,)
            )

            # Update the row with the new value
            for key, value in key_values.items():
                old_row.at[0, key] = value

            query_string = (
                f"INSERT INTO {self.name}.{namespace} SELECT * FROM old_row;"
            )
            self.con.execute(query_string)

        # Run triggers
        executed = set()
        for key, value in key_values.items():
            trigger_elem = TriggerElement(
                namespace=namespace, key=key, value=value
            )
            for trigger in self.triggers.get(f"{namespace}.{key}", []):
                if run_duplicates or trigger not in executed:
                    self.executeTrigger(id, trigger, trigger_elem)
                    executed.add(trigger)

        return id

    def duplicate(self, namespace: str, id: int) -> int:
        """Duplicate a record in a namespace. Doesn't run triggers.

        Args:
            namespace (str): The namespace to duplicate the record in.
            id (int): The id of the record to duplicate.

        Returns:
            int: The new id of the duplicated record.
        """
        new_id = self.getNewId(namespace)
        self.con.execute(
            f"INSERT INTO {self.name}.{namespace} SELECT {new_id} AS id, {id} AS derived_id, {', '.join(self.table_columns[namespace])} FROM {self.name}.{namespace} WHERE id = {id}"
        )
        return new_id

    def get(
        self, namespace: str, id: int, keys: typing.List[str], **kwargs
    ) -> typing.Any:
        """Get values for an id's keys in a namespace.
        TODO: Handle complex types.

        Args:
            namespace (str): The namespace to get the value from.
            id (int): The id of the record to get the value for.
            keys (typing.List[str]): The keys to get the values for.

        Keyword Args:
            caller_id (int, optional): The id of the caller. Defaults to None.
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
            if caller_id > id:
                raise ValueError(
                    f"Caller id {caller_id} is greater than id {id}!"
                )

        if not kwargs.get("include_derived", False):
            res = self.con.execute(
                f"SELECT {', '.join(keys)} FROM {self.name}.{namespace} WHERE id = {id}"
            ).fetchone()
            res_dict = {k: v for k, v in zip(keys, res)}
            res_dict.update({"id": id})
            return res_dict

        # Recursively get derived ids
        id_res = self.con.execute(
            f"SELECT id FROM {self.name}.{namespace} WHERE derived_id = {id}"
        ).fetchall()
        id_res = [i[0] for i in id_res]
        all_ids = [id] + id_res
        while len(id_res) > 0:
            id_res = self.con.execute(
                f"SELECT id FROM {self.name}.{namespace} WHERE derived_id IN ({', '.join([str(i) for i in id_res])})"
            ).fetchall()
            id_res = [i[0] for i in id_res]
            all_ids.extend(id_res)

        if kwargs.get("filter_null", True):
            return self.con.execute(
                f"SELECT {', '.join(keys)} FROM {self.name}.{namespace} WHERE id IN ({', '.join([str(i) for i in all_ids])}) AND {' AND '.join([f'{k} IS NOT NULL' for k in keys])}"
            ).fetchdf()

        else:
            return self.con.execute(
                f"SELECT {', '.join(keys)} FROM {self.name}.{namespace} WHERE id IN ({', '.join([str(i) for i in all_ids])})"
            ).fetchdf()

    def mget(
        self,
        namespace: str,
        ids: typing.List[int],
        keys: typing.List[str],
        **kwargs,
    ) -> pd.DataFrame:
        """Get multiple values for keys in a namespace.
        TODO: Handle complex types.

        Args:
            namespace (str): The namespace to get the value from.
            ids (typing.List[int]): The ids of the records to get the value for.
            keys (typing.List[str]): The keys to get the values for.

        Keyword Args:
            caller_id (int, optional): The id of the caller. Defaults to None.
            Used to prevent leakage, i.e., looking at data that has not been
            generated yet.

        Returns:
            pd.DataFrame: The values for the key.
        """
        # Check that there is no leakage
        if kwargs.get("caller_id") is not None:
            caller_id = kwargs.get("caller_id")
            if caller_id > max(ids):
                raise ValueError(
                    f"Caller id {caller_id} is greater than id {id}!"
                )

        return (
            self.con.execute(
                f"SELECT {', '.join(keys)} FROM {self.name}.{namespace} WHERE id IN ({', '.join([str(id) for id in ids])})"
            )
            .fetchdf()
            .dropna()
            .reset_index(drop=True)
        )

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
            caller_time = self.con.execute(
                f"SELECT ts FROM {self.name}.{caller_namespace} WHERE id = ?",
                (caller_id,),
            ).fetchone()[0]
            res = self.con.execute(
                f"SELECT id FROM {self.name}.{namespace} WHERE {key} = ? AND ts < ?",
                (value, caller_time),
            ).fetchall()
            return [r[0] for r in res]

        # Otherwise, just return all the ids
        res = self.con.execute(
            f"SELECT id FROM {self.name}.{namespace} WHERE {key} = ?",
            (value,),
        ).fetchall()
        return [r[0] for r in res]
