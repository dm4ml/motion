import duckdb
import typing

CONNECTIONS = {}


def get_or_create_store(name: str) -> typing.Any:
    """Get or create a store with the given name.

    Args:
        name (str): The name of the store to get or create.

    Returns:
        typing.Any: The store.
    """
    if name not in CONNECTIONS:
        con = duckdb.connect(f"{name}.db")
        CONNECTIONS[name] = Store(name, con)

    return CONNECTIONS[name]


class Store(object):
    def __init__(self, name: str, con):
        self.name = name
        self.con = con

    def addNamespace(self, name: str, schema: typing.Any) -> None:
        """Add a namespace to the store.

        Args:
            name (str): The name of the namespace.
            schema (typing.Any): The schema of the namespace.
        """
        self.con.execute(
            f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, {schema})"
        )

    def deleteNamespace(self, name: str) -> None:
        """Delete a namespace from the store.

        Args:
            name (str): The name of the namespace.
        """
        self.con.execute(f"DROP TABLE {name}")

    def addTrigger(self):
        pass
