from __future__ import annotations

import os
import shutil
from subprocess import run

import click

import motion

MOTION_HOME = os.environ.get("MOTION_HOME", os.path.expanduser("~/.cache/motion"))


@click.group()
def motioncli() -> None:
    pass


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
    # Return error if name is not valid
    name = name.strip().lower()
    if len(name.split(" ")) > 1:
        click.echo("Name cannot contain spaces.")
        return

    if os.path.exists(name):
        click.echo(f"Directory {name} already exists.")
        return

    # Copy over the example project
    shutil.copytree(os.path.join(os.path.dirname(__file__), "exampleproj"), name)

    # Create store setup file
    with open(os.path.join(name, "mconfig.py"), "w") as f:
        f.write(
            open(
                os.path.join(
                    os.path.dirname(__file__),
                    "exampleproj/mconfig.py",
                ),
            )
            .read()
            .replace("{0}", name)
            .replace("{1}", author)
        )

    click.echo("Created a project successfully.")


@motioncli.command("serve")
@click.argument("host", required=False, default="0.0.0.0")
@click.argument("port", required=False, default=8000)
@click.option(
    "logging_level",
    "--logging-level",
    "-l",
    default="INFO",
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
