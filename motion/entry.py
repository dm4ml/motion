import os
import pandas as pd
import requests

from enum import Enum
from motion.store import Store
from motion.api import create_app
from multiprocessing import Process

import colorlog
import io
import json
import logging
import pyarrow as pa
import sys
import time
import typing
import uuid
import uvicorn

from fastapi.testclient import TestClient
from fastapi import FastAPI


MOTION_HOME = os.environ.get(
    "MOTION_HOME", os.path.expanduser("~/.cache/motion")
)
logger = logging.getLogger(__name__)


def init(
    mconfig: dict, disable_cron_triggers: bool = False, session_id: str = None
) -> Store:
    """Initialize the motion store.

    Args:
        mconfig (dict): The motion configuration.
        disable_cron_triggers (bool, optional): Whether to disable cron triggers. Used during testing. Defaults to False.
        prod (bool, optional): Whether to run in production mode. Defaults to False.

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

    if session_id is None:
        session_id = str(uuid.uuid4())

    store = Store(
        name,
        session_id=session_id,
        datastore_prefix=os.path.join(MOTION_HOME, "datastores"),
        checkpoint=checkpoint,
        disable_cron_triggers=disable_cron_triggers,
    )

    # Create relations
    for relation, schema in mconfig["relations"].items():
        store.addrelation_pa(relation, schema)

    # Create triggers
    for trigger, keys in mconfig["triggers"].items():
        params = mconfig.get("trigger_params", {}).get(trigger, {})

        store.addTrigger(
            name=trigger.__name__,
            keys=keys,
            trigger=trigger,
            params=params,
        )

    # Start store
    store.start()

    return store


def serve(
    mconfig: dict, host="0.0.0.0", port=8000, motion_logging_level="INFO"
):
    """Serve a motion application.

    Args:
        mconfig (dict): The motion configuration.
        host (str, optional): The host to serve on. Defaults to "0.0.0.0".
        port (int, optional): The port to serve on. Defaults to 8000.
        motion_logging_level (str, optional): The logging level for motion.
    """
    configureLogging(motion_logging_level)
    store = init(mconfig, session_id="PRODUCTION")
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
    disable_cron_triggers: bool = False,
    motion_logging_level: str = "DEBUG",
    session_id: str = None,
):
    """Test a motion application. This will run the application
    and then shut it down.

    Args:
        mconfig (dict): Config for the motion application.
        wait_for_triggers (list, optional): Defaults to [].
        disable_cron_triggers (bool, optional): Defaults to False.
        motion_logging_level (str, optional): Defaults to "DEBUG".
        session_id (str, optional): Defaults to None.
    """
    if wait_for_triggers and disable_cron_triggers:
        raise ValueError(
            "Cannot wait for triggers if cron triggers are disabled."
        )

    configureLogging(motion_logging_level)
    store = init(
        mconfig,
        disable_cron_triggers=disable_cron_triggers,
        session_id=session_id,
    )
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
            self.session_id = self.store.session_id

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
            self.session_id = requests.get(self.server + "/session_id/").json()

    def close(self, wait=True):
        if isinstance(self.server, FastAPI):
            self.store.stop(wait=wait)

    def __del__(self):
        self.close(wait=False)

    def getWrapper(self, dest, **kwargs):
        if isinstance(self.server, FastAPI):
            with TestClient(self.server) as client:
                response = client.request("get", dest, json=kwargs)
        else:
            response = requests.get(self.server + dest, json=kwargs)

        if response.status_code != 200:
            raise Exception(response.content)

        with io.BytesIO(response.content) as data:
            if response.headers["content-type"] == "application/octet-stream":
                df = pd.read_parquet(data, engine="pyarrow")
                return df

            if response.headers["content-type"] == "application/json":
                return json.loads(response.content)

    def postWrapper(self, dest, data, files=None):
        if isinstance(self.server, FastAPI):
            with TestClient(self.server) as client:
                response = client.request("post", dest, data=data, files=files)
        else:
            response = requests.post(
                self.server + dest, data=data, files=files
            )

        if response.status_code != 200:
            raise Exception(response.content)

        return response.json()

    def waitForTrigger(self, trigger: str):
        """Wait for a trigger to fire.

        Args:
            trigger (str): The name of the trigger.
        """
        return self.postWrapper(
            "/wait_for_trigger/", data={"trigger": trigger}
        )

    def get(self, **kwargs):
        response = self.getWrapper("/get/", **kwargs)
        if not kwargs.get("as_df", False):
            return response.to_dict(orient="records")
        return response

    def mget(self, **kwargs):
        response = self.getWrapper("/mget/", **kwargs)
        if not kwargs.get("as_df", False):
            return response.to_dict(orient="records")
        return response

    def set(self, **kwargs):
        # Convert enums to their values
        for key, value in kwargs["key_values"].items():
            if isinstance(value, Enum):
                kwargs["key_values"].update({key: value.value})

        args = {
            "args": json.dumps(
                {k: v for k, v in kwargs.items() if k != "key_values"}
            )
        }

        # Turn key-values into a dataframe
        df = pd.DataFrame(kwargs["key_values"], index=[0])

        # Convert to parquet stream
        memory_buffer = io.BytesIO()
        df.to_parquet(memory_buffer, engine="pyarrow", index=False)
        memory_buffer.seek(0)

        return self.postWrapper(
            "/set/",
            data=args,
            files={
                "file": (
                    "key_values",
                    memory_buffer,
                    "application/octet-stream",
                )
            },
        )

    def getNewId(self, **kwargs):
        return self.getWrapper("/get_new_id/", **kwargs)

    def sql(self, **kwargs):
        return self.getWrapper("/sql/", **kwargs)
