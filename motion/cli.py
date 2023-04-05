import os
import shutil
from subprocess import run

import click

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
@click.argument("host", required=False, default="0.0.0.0")
@click.argument("port", required=False, default=5000)
@click.option(
    "logging_level",
    "--logging-level",
    "-l",
    default="WARNING",
    help="Logging level for motion. Can be DEBUG, INFO, WARNING, ERROR, CRITICAL.",
)
def serve(host: str, port: int, logging_level: str) -> None:
    """Serves a motion application."""

    # Check that the project is created
    if not os.path.exists("mconfig.py"):
        click.echo("Project is not created. Run `motion create` first.")
        return

    # Create object from mconfig.py
    config_code = open("mconfig.py").read() + "\nMCONFIG"

    import sys

    sys.path.append(os.getcwd())

    exec(config_code)
    mconfig = locals()["MCONFIG"]
    click.echo(f"Serving application {mconfig['application']['name']}...")

    # Serve the application
    motion.serve(mconfig, host=host, port=port, motion_logging_level=logging_level)


@motioncli.command("clear")
@click.argument("name", required=True)
def clear(name: str) -> None:
    """Removes the datastore for the given application."""
    # Remove directory at name
    MOTION_HOME = os.environ.get("MOTION_HOME", os.path.expanduser("~/.cache/motion"))
    dirname = os.path.join(MOTION_HOME, "datastores", name)
    if not os.path.exists(dirname):
        click.echo(f"Application {name} does not exist.")
        return

    shutil.rmtree(dirname)


@motioncli.command("test", help="Run pytest with the given arguments")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.option("--debug", is_flag=True, help="Enable debug output")
@click.argument("args", nargs=-1)
def test(verbose: bool, debug: bool, args: tuple) -> None:
    """Run pytest with the given arguments."""
    pytest_args = list(args)
    if verbose:
        pytest_args.append("-v")
    if debug:
        pytest_args.append("--debug")
    # pytest.main(pytest_args)

    run(["pytest", *pytest_args])


@motioncli.command("token", help="Generate a new API token")
def token() -> None:
    """Generate a new API token."""
    token = motion.create_token()
    click.echo(token)
