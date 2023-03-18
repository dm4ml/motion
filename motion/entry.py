import os
import requests

from motion.store import Store
from motion.api import create_app

import uvicorn


MOTION_HOME = os.environ.get(
    "MOTION_HOME", os.path.expanduser("~/.cache/motion")
)


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
    checkpoint = (
        mconfig["checkpoint"] if "checkpoint" in mconfig else "0 * * * *"
    )

    store = Store(
        name,
        datastore_prefix=os.path.join(MOTION_HOME, "datastores"),
        checkpoint=checkpoint,
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


def serve(mconfig: dict, host="0.0.0.0", port=8000):
    """Serve a motion application.

    Args:
        mconfig (dict): The motion configuration.
    """
    store = init(mconfig)

    # Start fastapi server
    app = create_app(store)
    uvicorn.run(app, host=host, port=port)

    # Log that the server is running
    os.makedirs(os.path.join(MOTION_HOME, "logs"), exist_ok=True)
    with open(os.path.join(MOTION_HOME, "logs", store.name), "a") as f:
        f.write(f"Server running at {host}:{port}")


def connect(name: str):
    """Connect to a motion application.

    Args:
        name (str): The name of the store.

    Returns:
        Store: The motion store.
    """
    #  Check logs
    os.makedirs(os.path.join(MOTION_HOME, "logs"), exist_ok=True)
    with open(os.path.join(MOTION_HOME, "logs", name), "r") as f:
        server = f.read().split(" ")[-1]

    return ClientConnection(name, server)


class ClientConnection(object):
    """A client connection to a motion store.

    Args:
        name (str): The name of the store.
    """

    def __init__(self, name: str, server: str):
        self.name = name
        self.server = server

    def get(self, **kwargs):
        dest = self.server + "/get/"
        return requests.get(dest, data=kwargs)

    def mget(self, **kwargs):
        dest = self.server + "/mget/"
        return requests.get(dest, data=kwargs)

    def set(self, **kwargs):
        dest = self.server + "/set/"
        return requests.post(dest, data=kwargs)

    def getNewId(self, **kwargs):
        dest = self.server + "/get_new_id/"
        return requests.post(dest, data=kwargs)

    def sql(self, **kwargs):
        dest = self.server + "/sql/"
        return requests.post(dest, data=kwargs)
