import click
import os

import shutil


@click.group()
def motion():
    pass


@motion.command("create")
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


@motion.command("deploy")
def deploy():
    # Check that the project is created
    if not os.path.exists("mconfig.py"):
        click.echo("Project is not created. Run `motion init` first.")
        return

    click.echo("Deploying your application...")
    click.echo("Deployed successfully.")


@motion.command("run")
def run():
    # Check that the project is created
    if not os.path.exists("mconfig.py"):
        click.echo("Project is not created. Run `motion init` first.")
        return

    click.echo("Running your application...")
    click.echo("Ran successfully.")
