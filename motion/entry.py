import logging
import os
import shutil
import typing
import uuid

import colorlog
import pandas as pd
import uvicorn

from motion.api import create_fastapi_app
from motion.client import ClientConnection
from motion.store import Store
from motion.utils import PRODUCTION_SESSION_ID


def create_token() -> str:
    """Creates a token for the API.

    Returns:
        str: The token.
    """
    return str(os.urandom(20).hex())


def get_logs(name: str, session_id: str = "") -> pd.DataFrame:
    """
    Gets the log table for a given application. Can optionally filter by session ID.

    Args:
        name (str): The name of the application.
        session_id (str, optional): The session ID to filter by. Defaults to "".

    Returns:
        pd.DataFrame: The log table.
    """

    MOTION_HOME = os.environ.get("MOTION_HOME", os.path.expanduser("~/.cache/motion"))
    dirname = os.path.join(MOTION_HOME, "datastores", name)
    if not os.path.exists(dirname):
        raise KeyError(f"Application {name} does not exist.")

    log_file = os.path.join(dirname, "logs.parquet")
    if not os.path.exists(log_file):
        raise ValueError(f"Application {name} does not have any logs.")

    log_table = pd.read_parquet(log_file)
    if session_id:
        log_table = log_table[log_table["session_id"] == session_id]

    return log_table


def create_example_app(name: str, author: str) -> None:
    """Creates a motion app from examples."""
    name = name.strip().lower()

    if name == "cooking":
        # Copy the example project
        shutil.copytree(
            os.path.join(os.path.dirname(__file__), f"examples/{name}"), name
        )

        # Create config
        with open(os.path.join(name, "mconfig.py"), "w") as f:
            f.write(
                open(
                    os.path.join(
                        os.path.dirname(__file__),
                        f"examples/{name}/mconfig.py",
                    ),
                )
                .read()
                .replace("{1}", author)
            )

    else:
        raise ValueError(f"Example application {name} does not exist.")


def create_app(name: str, author: str) -> None:
    """Creates a motion app."""
    name = name.strip().lower()
    if len(name.split(" ")) > 1:
        raise ValueError("Name cannot contain spaces.")

    if os.path.exists(name):
        raise ValueError(f"Directory {name} already exists.")

    # Copy over the example project
    shutil.copytree(os.path.join(os.path.dirname(__file__), "examples/basic"), name)

    # Create store setup file
    with open(os.path.join(name, "mconfig.py"), "w") as f:
        f.write(
            open(
                os.path.join(
                    os.path.dirname(__file__),
                    "examples/basic/mconfig.py",
                ),
            )
            .read()
            .replace("{0}", name)
            .replace("{1}", author)
        )


def init(
    mconfig: dict,
    disable_triggers: typing.List[str] = [],
    session_id: str = "",
) -> Store:
    """Initializes a motion application by creating a data store and adding relations and triggers.

    Args:
        mconfig (dict): The motion configuration.
        disable_triggers (typing.List[str], optional): A list of triggers to disable. Defaults to [].
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

    checkpoint = mconfig["checkpoint"] if "checkpoint" in mconfig else "0 * * * *"

    if session_id == "":
        session_id = str(uuid.uuid4())

    MOTION_HOME = os.environ.get("MOTION_HOME", os.path.expanduser("~/.cache/motion"))

    # Check that disabled triggers exist
    all_trigger_names = [t.__name__ for t in mconfig["triggers"]]
    for trigger_name in disable_triggers:
        if trigger_name not in all_trigger_names:
            raise ValueError(
                f"Trigger {trigger_name} specified in disable_triggers list does not exist."
            )

    store = Store(
        name,
        session_id=session_id,
        datastore_prefix=os.path.join(MOTION_HOME, "datastores"),
        checkpoint=checkpoint,
        disable_triggers=disable_triggers,
    )

    # Create relations
    for relation in mconfig["relations"]:
        store.addrelation_pa(relation.__name__, relation)

    # Create triggers
    for trigger in mconfig["triggers"]:
        params = mconfig.get("trigger_params", {}).get(trigger, {})

        store.addTrigger(
            name=trigger.__name__,
            trigger=trigger,
            params=params,
        )

    # Start store
    store.start()

    return store


def serve(
    mconfig: dict,
    host: str = "0.0.0.0",
    port: int = 5000,
    motion_logging_level: str = "INFO",
) -> None:
    """Serves a Motion application. Uses the MOTION_API_TOKEN environment variable to authenticate API requests.

    Args:
        mconfig (dict): The motion config, found in the mconfig.py file.
        host (str, optional): The host to serve on. Defaults to "0.0.0.0".
        port (int, optional): The port to serve on. Defaults to 5000.
        motion_logging_level (str, optional): The logging level for motion. Can be one of "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL". Defaults to "INFO".
    """
    configureLogging(motion_logging_level)
    store = init(mconfig, session_id=PRODUCTION_SESSION_ID)
    serve_store(store, host, port)


def serve_store(store: Store, host: str, port: int) -> None:
    # Start fastapi server
    app = create_fastapi_app(store)
    uvicorn.run(app, host=host, port=port)


def configureLogging(level: str) -> None:
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
    wait_for_triggers: typing.List[str] = [],
    disable_triggers: typing.List[str] = [],
    motion_logging_level: str = "WARNING",
    session_id: str = "",
) -> ClientConnection:
    """Creates a test connection to a Motion application, defined by a mconfig. This will run the application and then shut it down when the connection goes out of scope. Uses the MOTION_API_TOKEN environment variable to authenticate.

    Args:
        mconfig (dict): Config for the Motion application, found in the mconfig.py file.
        wait_for_triggers (typing.List[str], optional): List of cron-scheduled trigger names to wait for a first completion of. Typically used to wait for a first scrape of data.
        disable_triggers (typing.List[str], optional): List of cron-scheduled trigger names to disable. Typically used to disable scrapes of data.
        motion_logging_level (str, optional): Logging level for motion. Can be one of "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL". Defaults to "WARNING". Use "INFO" if you want to see all trigger execution logs.
        session_id (str, optional): Session ID to use for this connection. Defaults to a random UUID if empty.

    Returns:
        connection (motion.ClientConnection): A cannection to the Motion application.
    """
    if len(set(wait_for_triggers).intersection(set(disable_triggers))) > 0:
        raise ValueError(
            f"Cannot wait for triggers that are disabled. Please remove the following triggers from either the wait_for_triggers or disable_triggers lists: {set(wait_for_triggers).intersection(set(disable_triggers))}"
        )

    configureLogging(motion_logging_level)
    store = init(
        mconfig,
        disable_triggers=disable_triggers,
        session_id=session_id,
    )
    app = create_fastapi_app(store, testing=True)

    connection = ClientConnection(
        mconfig["application"]["name"],
        server=app,
        bearer_token=os.environ.get("MOTION_API_TOKEN", ""),
    )
    connection.addStore(store)

    for trigger in wait_for_triggers:
        connection.waitForTrigger(trigger)

    return connection


def connect(
    name: str,
    host: str = "0.0.0.0",
    port: int = 5000,
    wait_for_triggers: typing.List[str] = [],
    motion_api_token: str = "",
) -> ClientConnection:
    """Connects to a Motion application that is already being served.

    Args:
        name (str): The name of the Motion application.
        host (str, optional): The host of the Motion application. Defaults to localhost.
        port (int, optional): The port of the Motion application. Defaults to 5000.
        wait_for_triggers (typing.List[str], optional): List of cron-scheduled trigger names to wait for a first completion of. Typically used to wait for a first scrape of data.
        motion_api_token (str, optional): API token set as the environment variable on the host serving the Motion application. If not provided as an argument, the token will be read from environment (possibly throwing an error if the environment doesn't have an API token defined).

    Returns:
        connection (motion.ClientConnection): A connection to the Motion application.
    """
    #  Check logs
    MOTION_API_TOKEN = (
        motion_api_token if motion_api_token else os.environ.get("MOTION_API_TOKEN", "")
    )

    server = "http://" + host + ":" + str(port)
    connection = ClientConnection(name, server, bearer_token=MOTION_API_TOKEN)
    for trigger in wait_for_triggers:
        connection.waitForTrigger(trigger)

    return connection
