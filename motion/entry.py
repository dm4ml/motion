import os

from motion.store import Store


def init(
    mconfig: dict, memory: bool = True, datastore_prefix: str = "datastores"
) -> Store:
    """Initialize the motion store.

    Args:
        mconfig (dict): The motion configuration.
        memory (bool): Whether to use memory or not.
        datastore_prefix (str): The prefix for the datastore.

    Returns:
        Store: The motion store.
    """
    # TODO(shreyashankar): use version to check for updates when
    # needing to reinit

    name = mconfig["application"]["name"]
    author = mconfig["application"]["author"]

    store = Store(name, memory=memory, datastore_prefix=datastore_prefix)

    # Create namespaces
    for namespace, schema in mconfig["namespaces"].items():
        store.addNamespace(namespace, schema)

    # Create triggers
    for trigger, keys in mconfig["triggers"].items():
        store.addTrigger(
            name=trigger.__name__,
            keys=keys,
            trigger=trigger,
        )

    return store
