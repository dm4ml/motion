import dill
import duckdb
import inspect
import logging
import os
import pandas as pd
import typing

from collections import namedtuple
from enum import Enum
from motion import Trigger
from motion.dbcon import Connection
from motion.trigger import TriggerElement, TriggerFn

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
        self.addLogTable()

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

    def cursor(self):
        """Generates a new cursor for the database, with triggers and all.

        Returns:
            Connection: The cursor.
        """
        return Connection(
            self.name, self.con, self.table_columns, self.triggers
        )

    def addLogTable(self):
        """Creates a table to store trigger logs."""

        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self.name}.logs(executed_time DATETIME DEFAULT CURRENT_TIMESTAMP, trigger_name VARCHAR, trigger_version INTEGER, trigger_action VARCHAR, namespace VARCHAR, id INTEGER, trigger_key VARCHAR)"
        )

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
            if not issubclass(trigger, Trigger):
                raise ValueError(
                    f"Trigger class must implement the Trigger interface."
                )

        else:
            raise ValueError(
                f"Trigger {name} must be a function or class. Got {type(trigger)}."
            )

        # Add the trigger to the store
        self.trigger_names[name] = keys

        version = self.con.execute(
            f"SELECT MAX(trigger_version) FROM {self.name}.logs WHERE trigger_name = '{name}';"
        ).fetchone()
        version = version[0] if version[0] else 0
        trigger_exec = (
            trigger(self.cursor(), name, version)
            if inspect.isclass(trigger)
            else trigger
        )
        self.trigger_fns[name] = trigger_exec

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
