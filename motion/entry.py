import os

from motion.store import Store


def init(mconfig: dict) -> Store:
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
    datastore_prefix = (
        mconfig["datastore_prefix"]
        if "datastore_prefix" in mconfig
        else "datastores"
    )
    checkpoint = (
        mconfig["checkpoint"] if "checkpoint" in mconfig else "0 * * * *"
    )

    store = Store(
        name, datastore_prefix=datastore_prefix, checkpoint=checkpoint
    )

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

    # Start store
    store.start()

    return store
