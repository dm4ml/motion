import dill
import duckdb
import inspect
import os
import typing

from collections import namedtuple
from motion import Transform

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


TriggerFn = namedtuple("TriggerFn", ["name", "fn", "transform"])


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
        sql = schema.format_create_table_sql(f"{self.name}.{name}")
        self.con.execute(sql)

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
        key: str,
        trigger: typing.Union[typing.Callable, type],
    ) -> None:
        """Adds a trigger to the store.

        Args:
            name (str): Trigger name.
            key (str): Name of the key to triger on. Formatted as "namespace.key".
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
        self.trigger_names[name] = key
        self.trigger_fns[name] = trigger(self)
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
        key = self.trigger_names[name]
        fn = self.trigger_fns[name]
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
