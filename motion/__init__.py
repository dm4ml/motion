from motion.component import Component
from motion.utils import (
    UpdateEventGroup,
    clear_instance,
    clear_dev_instances,
    inspect_state,
    get_instances,
    RedisParams,
)
from motion.instance import ComponentInstance
from motion.migrate import StateMigrator
from motion.df import MDataFrame
from motion.copy_utils import copy_db
from motion.server.application import Application
from motion.mtable import MTable


# Set up an atexit hook to clear all instances in dev mode
import os

if os.getenv("MOTION_ENV", "dev") == "dev":
    import atexit
    from rich.console import Console

    def cleanup_dev() -> None:
        # Print cleanup message with rich spinner
        console = Console()
        with console.status(
            "[bold green]Performing cleanup...[/bold green]", spinner="dots"
        ):
            num_deleted = clear_dev_instances()

        plural = "s" if num_deleted != 1 else ""
        console.print(
            f"[bold green]Cleanup finished successfully! Deleted {num_deleted} instance{plural}.[/bold green]"
        )

    # Register the cleanup function
    atexit.register(cleanup_dev)

__all__ = [
    "Component",
    "UpdateEventGroup",
    "ComponentInstance",
    "clear_instance",
    "inspect_state",
    "StateMigrator",
    "get_instances",
    "MDataFrame",
    "copy_db",
    "RedisParams",
    "Application",
    "MTable",
]
