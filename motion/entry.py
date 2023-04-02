import logging
import os
import shutil
import uuid

import colorlog
import uvicorn

from motion.api import create_fastapi_app
from motion.client import ClientConnection
from motion.store import Store


def create_token() -> str:
    """Create a token for the API.

    Returns:
        str: The token.
    """
    return str(uuid.uuid4())


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
    mconfig: dict, disable_cron_triggers: bool = False, session_id: str = ""
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

    checkpoint = mconfig["checkpoint"] if "checkpoint" in mconfig else "0 * * * *"

    if session_id == "":
        session_id = str(uuid.uuid4())

    MOTION_HOME = os.environ.get("MOTION_HOME", os.path.expanduser("~/.cache/motion"))

    store = Store(
        name,
        session_id=session_id,
        datastore_prefix=os.path.join(MOTION_HOME, "datastores"),
        checkpoint=checkpoint,
        disable_cron_triggers=disable_cron_triggers,
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
    port: int = 8000,
    motion_logging_level: str = "INFO",
) -> None:
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


def serve_store(store: Store, host: str, port: int) -> None:
    # Log that the server is running
    MOTION_HOME = os.environ.get("MOTION_HOME", os.path.expanduser("~/.cache/motion"))

    os.makedirs(os.path.join(MOTION_HOME, "logs"), exist_ok=True)
    with open(os.path.join(MOTION_HOME, "logs", store.name), "a") as f:
        f.write(f"Server running at {host}:{port}")

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
    wait_for_triggers: list = [],
    disable_cron_triggers: bool = False,
    motion_logging_level: str = "WARNING",
    session_id: str = "",
) -> ClientConnection:
    """Test a motion application. This will run the application
    and then shut it down.

    Args:
        mconfig (dict): Config for the motion application.
        wait_for_triggers (list, optional): Defaults to [].
        disable_cron_triggers (bool, optional): Defaults to False.
        motion_logging_level (str, optional): Defaults to "WARNING".
        session_id (str, optional): Defaults to "".
    """
    if wait_for_triggers and disable_cron_triggers:
        raise ValueError("Cannot wait for triggers if cron triggers are disabled.")

    configureLogging(motion_logging_level)
    store = init(
        mconfig,
        disable_cron_triggers=disable_cron_triggers,
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
    wait_for_triggers: list = [],
    motion_api_token: str = "",
) -> ClientConnection:
    """Connect to a motion application.

    Args:
        name (str): The qname of the store.
        wait_for_triggers (list, optional): Defaults to [].
        motion_api_token (str, optional): Defaults to "". If not provided, the token will be read from environment.

    Returns:
        Store: The motion store.
    """
    #  Check logs
    MOTION_HOME = os.environ.get("MOTION_HOME", os.path.expanduser("~/.cache/motion"))
    MOTION_API_TOKEN = (
        motion_api_token if motion_api_token else os.environ.get("MOTION_API_TOKEN", "")
    )

    os.makedirs(os.path.join(MOTION_HOME, "logs"), exist_ok=True)
    try:
        with open(os.path.join(MOTION_HOME, "logs", name)) as f:
            server = f.read().split(" ")[-1]
    except FileNotFoundError:
        raise Exception(
            f"Could not find a server for {name}. Please run `motion serve` first."
        )

    connection = ClientConnection(name, server, bearer_token=MOTION_API_TOKEN)
    for trigger in wait_for_triggers:
        connection.waitForTrigger(trigger)

    return connection
