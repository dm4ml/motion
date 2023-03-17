import click
import importlib
import inspect
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


@motioncli.command("deploy")
def deploy():
    # Check that the project is created
    if not os.path.exists("mconfig.py"):
        click.echo("Project is not created. Run `motion create` first.")
        return

    click.echo("Deploying your application...")
    click.echo("Deployed successfully.")


@motioncli.command("run")
@click.argument("script_name", required=True)
def run(script_name):
    # Check that the project is created
    if not os.path.exists("mconfig.py"):
        click.echo("Project is not created. Run `motion create` first.")
        return

    if not os.path.exists(os.path.join("scripts", script_name)):
        click.echo(f"Script {script_name} does not exist.")
        return

    config_code = open("mconfig.py", "r").read() + "\nMCONFIG"
    exec(config_code, globals(), locals())
    mconfig = locals()["MCONFIG"]

    store = motion.init(mconfig)

    script_contents = open(os.path.join("scripts", script_name), "r").read()
    exec(script_contents, globals(), locals())

    # Find instances of MotionScript
    for key, value in locals().items():
        if (
            inspect.isclass(value)
            and issubclass(value, MotionScript)
            and key != "MotionScript"
        ):
            # script = value(store)
            exec(
                script_contents
                + "import motion; store = motion.init(MCONFIG); script = value(store); script.run()",
                globals(),
                locals(),
            )

    # click.echo("Running your application...")
    # click.echo(script_contents)

    # click.echo(exec(script_contents, globals(), {"MCONFIG": MCONFIG}))

    # click.echo("Ran successfully.")
