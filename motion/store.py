import dill
import duckdb
import inspect
import logging
import os
import typing

from collections import namedtuple
from motion import Transform, Schema
from motion.transform import TriggerElement

CONNECTIONS = {}


def get_or_create_store(name: str) -> typing.Any:
    """Get or create a store with the given name.

    Args:
        name (str): The name of the store to get or create.

    Returns:
        typing.Any: The store.
    """
    if name not in CONNECTIONS:
        CONNECTIONS[name] = Store(name)

    return CONNECTIONS[name]


TriggerFn = namedtuple("TriggerFn", ["name", "fn", "isTransform"])


class Store(object):
    def __init__(self, name: str):
        self.name = name
        self.con = duckdb.connect(":memory:")
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {name}")
        # self.con = duckdb.connect(f"datastores/{name}/duck.db")
        self.triggers = (
            dill.load(open(f"datastores/{name}.triggers", "rb"))
            if os.path.exists(f"datastores/{name}.triggers")
            else {}
        )
        self.trigger_names = (
            dill.load(open(f"datastores/{name}.trigger_names", "rb"))
            if os.path.exists(f"datastores/{name}.trigger_names")
            else {}
        )
        self.trigger_fns = (
            dill.load(open(f"datastores/{name}.trigger_fns", "rb"))
            if os.path.exists(f"datastores/{name}.trigger_fns")
            else {}
        )

    # def __del__(self):
    #     # Close connection and persist triggers
    #     self.con.close()
    #     dill.dump(
    #         self.triggers, open(f"datastores/{self.name}.triggers", "wb")
    #     )
    #     dill.dump(
    #         self.trigger_names,
    #         open(f"datastores/{self.name}.trigger_names", "wb"),
    #     )
    #     dill.dump(
    #         self.trigger_fns, open(f"datastores/{self.name}.trigger_fns", "wb")
    #     )

    def addNamespace(self, name: str, schema: typing.Any) -> None:
        """Add a namespace to the store.
        TODO(shreya): Error checking

        Args:
            name (str): The name of the namespace.
            schema (typing.Any): The schema of the namespace.
        """
        stmts = schema.formatCreateStmts(f"{self.name}.{name}")
        for stmt in stmts:
            logging.info(stmt)
            self.con.execute(stmt)

    def deleteNamespace(self, name: str) -> None:
        """Delete a namespace from the store.
        TODO(shreya): Error checking

        Args:
            name (str): The name of the namespace.
        """
        self.con.execute(f"DROP TABLE {self.name}.{name}")

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
            trigger (typing.Union[typing.Callable, type]): Function or class to execute when the trigger is fired. If function, must take in the id of the row that triggered the trigger and a reference to the store object (in this order). If class, must implement the Transform interface.

        Raises:
            ValueError: If there is already a trigger with the given name.
        """
        if name in self.trigger_names:
            raise ValueError(
                f"Trigger {name} already exists. Please delete it and try again."
            )

        if inspect.isfunction(trigger):
            # Check that the function signature is correct
            if len(inspect.signature(trigger).parameters) != 2:
                raise ValueError(
                    f"Trigger function must take in 2 arguments: id and store."
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
        for key in keys:
            self.triggers[key] = self.triggers.get(key, []) + [
                TriggerFn(name, trigger, inspect.isclass(trigger))
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

    def getMaxId(self, namespace: str) -> int:
        """Get the max id for a namespace.

        Args:
            namespace (str): The namespace to get the max id for.

        Returns:
            int: The max id.
        """
        max_id = (
            self.con.execute(
                f"SELECT MAX(id) FROM {self.name}.{namespace}"
            ).fetchone()[0]
            or -1
        )
        return max_id

    def getNewId(self, namespace: str) -> int:
        """Get a new id for a namespace.

        Args:
            namespace (str): The namespace to get the new id for.

        Returns:
            int: The new id.
        """
        return self.getMaxId(namespace) + 1

    def exists(self, namespace: str, primary_key: dict) -> bool:
        """Determine if a record exists in a namespace.

        Args:
            namespace (str): The namespace to check.
            primary_key (dict): The primary key of the record.

        Returns:
            bool: True if the record exists, False otherwise.
        """
        stmt = f"SELECT COUNT(*) FROM {self.name}.{namespace} WHERE "
        for k, v in primary_key.items():
            stmt += f"{k} = {v} AND "
        stmt = stmt[:-5]
        return self.con.execute(stmt).fetchone()[0] > 0

    def executeTrigger(self, trigger: TriggerFn, trigger_elem: TriggerElement):
        """Execute a trigger.

        Args:
            trigger (TriggerFn): The trigger to execute.
            trigger_elem (TriggerElement): The element that triggered the trigger.
        """
        trigger_name, trigger_fn, isTransform = trigger
        logging.info(f"Running trigger {trigger_name}...")
        if not isTransform:
            trigger_fn(trigger_elem.id, self)

    def set(
        self,
        namespace: str,
        primary_key: typing.Dict,
        key: str,
        value: typing.Any,
    ) -> None:
        """Set a value for a key in a namespace.
        TODO(shreyashankar): Handle complex types.

        Args:
            namespace (str): The namespace to set the value in.
            primary_key (typing.Dict): The primary key of the record. In most cases it will just be the id, like {"id": 1}. In some cases, when there are page attributes, it will be {"id": 1, "_page": 1}.
            key (str): The key to set the value for.
            value (typing.Any): The value to set.
        """
        if not self.exists(namespace, primary_key):
            query_string = (
                f"INSERT INTO {self.name}.{namespace} ({', '.join(primary_key.keys())}, {key}) VALUES ({', '.join(['?'] * len(primary_key.values()))}, ?)",
                (*primary_key.values(), value),
            )

        else:
            query_string = (
                f"UPDATE {self.name}.{namespace} SET {key} = ? WHERE {' AND '.join([f'{k} = ?' for k in primary_key.keys()])}",
                (value, *primary_key.values()),
            )

        self.con.execute(*query_string)

        # Run triggers
        # TODO(shreya): implement this
        trigger_elem = TriggerElement(
            namespace=namespace, key=key, value=value
        )
        print(trigger_elem)
        for trigger in self.triggers.get(f"{namespace}.{key}", []):
            trigger_name, trigger_fn, isTransform = trigger
            logging.info(f"Running trigger {trigger_name}...")
            # if isTransform:
            #     self.set(namespace, primary_key, key, trigger_fn(value))
            # else:
            #     trigger_fn()
