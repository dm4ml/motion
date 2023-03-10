import click
import os

import shutil


@click.group()
def motion():
    pass


@motion.command("init")
@click.option(
    "--name",
    prompt="Your application name",
    help="The name of your application.",
)
def init(name):
    """Initializes a new application."""
    if os.path.exists(name):
        click.echo(f"Directory {name} already exists.")
        return

    os.mkdir(name)

    # Create directory structure
    # shutil.copytree(os.path.join(os.path.dirname(__file__), "example"), name)

    os.mkdir(os.path.join(name, "schemas"))
    with open(os.path.join(name, "schemas", "__init__.py"), "w") as f:
        f.write("")
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
        f.write("")
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
    with open(os.path.join(name, "config.py"), "w") as f:
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
        )

    click.echo("Initialized a project successfully.")


@motion.command("deploy")
def deploy():
    # Check that the project is initialized
    if not os.path.exists("config.py"):
        click.echo("Project is not initialized. Run `motion init` first.")
        return

    click.echo("Deploying your application...")
    click.echo("Deployed successfully.")


@motion.command("run")
def run():
    # Check that the project is initialized
    if not os.path.exists("config.py"):
        click.echo("Project is not initialized. Run `motion init` first.")
        return

    click.echo("Running your application...")
    click.echo("Ran successfully.")
