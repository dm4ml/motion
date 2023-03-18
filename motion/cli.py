import click
import importlib
import inspect
import logging
import motion
import os

import shutil

from motion import MotionScript
from subprocess import call


@click.group()
def motioncli():
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
def create(name, author):
    """Creates a new application."""
    # Return error if name is not valid
    name = name.strip().lower()
    if len(name.split(" ")) > 1:
        click.echo("Name cannot contain spaces.")
        return

    if os.path.exists(name):
        click.echo(f"Directory {name} already exists.")
        return

    os.mkdir(name)

    # Create directory structure
    # shutil.copytree(os.path.join(os.path.dirname(__file__), "example"), name)

    os.mkdir(os.path.join(name, "schemas"))
    with open(os.path.join(name, "schemas", "__init__.py"), "w") as f:
        f.write(
            open(
                os.path.join(
                    os.path.dirname(__file__),
                    "exampleproj/schemas/__init__.txt",
                ),
                "r",
            )
            .read()
            .replace("{0}", name)
        )
    with open(os.path.join(name, "schemas", "chat.py"), "w") as f:
        f.write(
            open(
                os.path.join(
                    os.path.dirname(__file__), "exampleproj/schemas/chat.py"
                ),
                "r",
            ).read()
        )

    os.mkdir(os.path.join(name, "triggers"))
    with open(os.path.join(name, "triggers", "__init__.py"), "w") as f:
        f.write(
            open(
                os.path.join(
                    os.path.dirname(__file__),
                    "exampleproj/triggers/__init__.txt",
                ),
                "r",
            )
            .read()
            .replace("{0}", name)
        )
    with open(os.path.join(name, "triggers", "chatbot.py"), "w") as f:
        f.write(
            open(
                os.path.join(
                    os.path.dirname(__file__),
                    "exampleproj/triggers/chatbot.py",
                ),
                "r",
            ).read()
        )

    # Create store setup file
    with open(os.path.join(name, "mconfig.py"), "w") as f:
        f.write(
            open(
                os.path.join(
                    os.path.dirname(__file__),
                    "exampleproj/config.txt",
                ),
                "r",
            )
            .read()
            .replace("{0}", name)
            .replace("{1}", author)
        )

    click.echo("Created a project successfully.")


@motioncli.command("serve")
def serve():
    # Check that the project is created
    if not os.path.exists("mconfig.py"):
        click.echo("Project is not created. Run `motion create` first.")
        return

    # Create object from mconfig.py
    config_code = open("mconfig.py", "r").read() + "\nMCONFIG"
    exec(config_code, globals(), locals())
    mconfig = locals()["MCONFIG"]
    click.echo(f"Serving application {mconfig['application']['name']}...")

    # Serve the application
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    motion.serve(mconfig, host="0.0.0.0", port=8000)
    click.echo("Served successfully.")


@motioncli.command("delete")
@click.argument("name", required=True)
def delete(name):
    # Remove directory at name
    dirname = os.path.join("~/.cache/motion", name)
    if not os.path.exists(dirname):
        click.echo(f"Application {name} does not exist.")
        return

    shutil.rmtree(dirname)
