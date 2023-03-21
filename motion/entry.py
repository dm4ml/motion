import os
import pandas as pd
import requests

from enum import Enum
from motion.store import Store
from motion.api import create_app
from multiprocessing import Process

import colorlog
import logging
import sys
import time
import typing
import uvicorn

from fastapi.testclient import TestClient
from fastapi import FastAPI


MOTION_HOME = os.environ.get(
    "MOTION_HOME", os.path.expanduser("~/.cache/motion")
)
logger = logging.getLogger(__name__)


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

    try:
        name = mconfig["application"]["name"]
        author = mconfig["application"]["author"]
    except Exception as e:
        raise Exception("Motion config must have application name and author.")

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
    serve_store(store, host, port)


def serve_store(store, host, port):
    # Log that the server is running
    os.makedirs(os.path.join(MOTION_HOME, "logs"), exist_ok=True)
    with open(os.path.join(MOTION_HOME, "logs", store.name), "a") as f:
        f.write(f"Server running at {host}:{port}")

    # Start fastapi server
    app = create_app(store)
    uvicorn.run(app, host=host, port=port)


def configureLogging(level: str):
    handler = logging.StreamHandler()

    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s %(levelname)-8s%(reset)s %(blue)s%(message)s",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
    )
    handler.setFormatter(formatter)
    logger = logging.getLogger("motion")
    logger.addHandler(handler)
    logger.setLevel(level)


def test(
    mconfig: dict,
    wait_for_triggers: list = [],
    motion_logging_level: str = "DEBUG",
):
    """Test a motion application. This will run the application
    and then shut it down.

    Args:
        mconfig (dict): Config for the motion application.
        wait_for_triggers (list, optional): Defaults to [].
        motion_logging_level (str, optional): Defaults to "DEBUG".
    """
    configureLogging(motion_logging_level)
    store = init(mconfig)
    app = create_app(store, testing=True)
    connection = ClientConnection(
        mconfig["application"]["name"], server=app, store=store
    )

    for trigger in wait_for_triggers:
        connection.waitForTrigger(trigger)

    return connection


def connect(name: str, wait_for_triggers: list = []):
    """Connect to a motion application.

    Args:
        name (str): The qname of the store.
        wait_for_triggers (list, optional): Defaults to [].

    Returns:
        Store: The motion store.
    """
    #  Check logs
    os.makedirs(os.path.join(MOTION_HOME, "logs"), exist_ok=True)
    try:
        with open(os.path.join(MOTION_HOME, "logs", name), "r") as f:
            server = f.read().split(" ")[-1]
    except FileNotFoundError:
        raise Exception(
            f"Could not find a server for {name}. Please run `motion serve` first."
        )

    connection = ClientConnection(name, server)
    for trigger in wait_for_triggers:
        connection.waitForTrigger(trigger)

    return connection


class ClientConnection(object):
    """A client connection to a motion store.

    Args:
        name (str): The name of the store.
    """

    def __init__(
        self, name: str, server: typing.Union[str, FastAPI], store=None
    ):
        self.name = name

        if isinstance(server, FastAPI):
            self.server = server
            self.store = store

        else:
            self.server = "http://" + server
            try:
                response = requests.get(self.server + "/ping/")
                if response.status_code != 200:
                    raise Exception(
                        f"Could not successfully connect to server for {self.name}; getting status code {response.status_code}."
                    )
            except requests.exceptions.ConnectionError:
                raise Exception(
                    f"Could not connect to server for {self.name} at {self.server}. Please run `motion serve` first."
                )

    def close(self, wait=True):
        if isinstance(self.server, FastAPI):
            self.store.stop(wait=wait)

    def __del__(self):
        self.close(wait=False)

    def getWrapper(self, dest, **kwargs):
        if isinstance(self.server, FastAPI):
            with TestClient(self.server) as client:
                response = client.request("get", dest, json=kwargs).json()
        else:
            response = requests.get(self.server + dest, json=kwargs).json()

        if kwargs.get("as_df", False):
            return pd.DataFrame(response)

        return response

    def postWrapper(self, dest, **kwargs):
        if isinstance(self.server, FastAPI):
            with TestClient(self.server) as client:
                response = client.request("post", dest, data=kwargs).json()
        else:
            response = requests.post(self.server + dest, data=kwargs).json()

        return response

    def waitForTrigger(self, trigger: str):
        """Wait for a trigger to fire.

        Args:
            trigger (str): The name of the trigger.
        """
        return self.postWrapper("/wait_for_trigger/", trigger=trigger)

    def get(self, **kwargs):
        return self.getWrapper("/get/", **kwargs)

    def mget(self, **kwargs):
        return self.getWrapper("/mget/", **kwargs)

    def set(self, **kwargs):
        # Convert enums to their values
        for key, value in kwargs["key_values"].items():
            if isinstance(value, Enum):
                kwargs["key_values"].update({key: value.value})

        # Merge key_values with kwargs
        kwargs.update(kwargs["key_values"])
        del kwargs["key_values"]

        return self.postWrapper("/set/", **kwargs)

    def getNewId(self, **kwargs):
        return self.postWrapper("/get_new_id/", **kwargs)

    def sql(self, **kwargs):
        return self.getWrapper("/sql/", **kwargs)
