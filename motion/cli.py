import os
import shutil

import click
from rich.console import Console
from rich.table import Table

import motion


@click.group()
def motioncli() -> None:
    pass


@motioncli.command("example")
@click.option(
    "--name",
    prompt="Example application name",
    help="One of the example applications to create. Can be: 'cooking'.",
)
@click.option(
    "--author",
    prompt="Your name",
    help="Author name.",
)
def example(name: str, author: str) -> None:
    """Creates a new application from an example."""
    motion.create_example_app(name, author)
    click.echo("Created a project successfully.")


@motioncli.command("create")
@click.option(
    "--name",
    prompt="Your application name",
    help="The name of your application.",
)
@click.option(
    "--author",
    prompt="Your name",
    help="Author name.",
)
def create(name: str, author: str) -> None:
    """Creates a new application."""
    motion.create_app(name, author)

    click.echo("Created a project successfully.")


@motioncli.command("serve")
@click.option("name", "--name", default="", help="Project name.")
@click.option("host", "--host", default="0.0.0.0", help="Host to serve on.")
@click.option("port", "--port", default=5000, help="Port to serve on.")
@click.option(
    "logging_level",
    "--logging-level",
    "-l",
    default="WARNING",
    help="Logging level for motion. Can be DEBUG, INFO, WARNING, ERROR, CRITICAL.",
)
def serve(name: str, host: str, port: int, logging_level: str) -> None:
    """Serves a Motion application."""

    # Check that the project is created
    config_path = os.path.join(name, "mconfig.py")
    if not os.path.exists(config_path):
        click.echo("Project is not created. Run `motion create` first.")
        return

    # Create object from mconfig.py
    config_code = open(config_path).read() + "\nMCONFIG"

    import sys

    sys.path.append(os.getcwd())

    exec(config_code)
    mconfig = locals()["MCONFIG"]
    click.echo(f"Serving application {mconfig['application']['name']}...")

    if name != "":
        assert name == mconfig["application"]["name"], "Name does not match."

    # Serve the application
    motion.serve(mconfig, host=host, port=port, motion_logging_level=logging_level)


@motioncli.command("clear")
@click.argument("name", required=True)
def clear(name: str) -> None:
    """Removes the data store for the given Motion application."""
    # Remove directory at name
    MOTION_HOME = os.environ.get("MOTION_HOME", os.path.expanduser("~/.cache/motion"))
    dirname = os.path.join(MOTION_HOME, "datastores", name)
    if not os.path.exists(dirname):
        click.echo(f"Application {name} does not exist.")
        return

    shutil.rmtree(dirname)


@motioncli.command("token", help="Generate a new API token")
def token() -> None:
    """Generate a new API token."""
    token = motion.create_token()
    click.echo(token)


@motioncli.command("logs", help="Show logs for a Motion application")
@click.argument("name", required=True)
@click.option(
    "session_id",
    "--session-id",
    help="Session ID to show logs for.",
    default="",
)
@click.option("limit", "--limit", help="Limit number of logs to show.", default=100)
def logs(name: str, session_id: str, limit: int) -> None:
    """Show logs for a Motion application."""
    # Read logs
    log_table = motion.get_logs(name, session_id=session_id)
    log_table = log_table.tail(limit)

    # create a Rich table and add columns
    console = Console()
    table = Table(show_header=True, header_style="bold magenta")
    for column_name in log_table.columns:
        if "trigger_" in column_name:
            table.add_column(column_name.replace("trigger_", ""))
        else:
            table.add_column(column_name)

    # add rows to the Rich table
    for _, row in log_table.iterrows():
        table.add_row(
            *[str(row[column_name]) for column_name in log_table.columns],
        )

    # print the Rich table
    console.print(table)
