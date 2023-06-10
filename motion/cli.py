import importlib
import json
import os
import sys
from datetime import datetime

import click
import redis
from rich.console import Console

from motion import clear_instance, inspect_state


@click.group()
def motioncli() -> None:
    """Motion commands."""
    pass


@motioncli.command(
    "vis",
    epilog="Example usage:\n motion vis main.py:MyComponent",
)
@click.argument(
    "filename",
    type=str,
    required=True,
)
@click.option(
    "--output",
    type=str,
    default="graph.json",
    help="JSON filename to output the component graph to.",
)
def visualize(filename: str, output: str) -> None:
    """Visualize a component."""
    red_x = "\u274C"  # Unicode code point for red "X" emoji
    if ":" not in filename:
        click.echo(
            f"{red_x} Component must be in the format " + "`filename:component`."
        )
        return

    # Remove the file extension if present
    module = filename.replace(".py", "")

    first, instance = module.split(":")
    if not first or not instance:
        click.echo(
            f"{red_x} Component must be in the format " + "`filename:component`."
        )
        return

    module_dir = os.getcwd()
    sys.path.insert(0, module_dir)
    module = importlib.import_module(first)  # type: ignore

    # Get the class instance
    try:
        class_instance = getattr(module, instance)
    except AttributeError as e:
        click.echo(f"{red_x} {e}")
        return

    # Get the graph
    graph = class_instance.get_graph()

    # Dump the graph to a file with the date
    ts = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    out_filename = f"{ts}_{instance}_{output}"
    checkmark = "\u2705"  # Unicode code point for checkmark emoji

    with open(out_filename, "w") as f:
        json.dump(graph, f, indent=4)
        click.echo(f"{checkmark} Graph dumped to {out_filename}")


@motioncli.command(
    "clear", epilog="Example usage:\n motion clear MyComponent__myinstance"
)
@click.argument("instance", type=str, required=True)
def clear(instance: str) -> None:
    """Clears the state and cached results for a component instance.

    Args:
        instance (str): Instance name of the component to clear.
            In the form `componentname__instancename`.
    """
    console = Console()
    red_x = "\u274C"
    checkmark = "\u2705"  # Unicode code point for checkmark emoji
    with console.status("Clearing instance", spinner="dots"):
        try:
            found = clear_instance(instance)
        except ValueError as e:
            click.echo(f"{red_x} {e}")
            return
        except redis.exceptions.ConnectionError as e:
            click.echo(f"{red_x} {e}")
            return

    if not found:
        click.echo(f"{red_x} Instance {instance} not found.")

    else:
        click.echo(f"{checkmark} Instance {instance} cleared.")


@motioncli.command(
    "inspect", epilog="Example usage:\n motion inspect MyComponent__myinstance"
)
@click.argument("instance", type=str, required=True)
def inspect(instance: str) -> None:
    """Prints the saved state for a component instance. Does not apply
    any loadState() transformations.

    Args:
        instance (str): Instance name of the component to clear.
            In the form `componentname__instancename`.
    """
    console = Console()
    red_x = "\u274C"
    checkmark = "\u2705"  # Unicode code point for checkmark emoji
    with console.status("Clearing instance", spinner="dots"):
        try:
            state = inspect_state(instance)
        except ValueError as e:
            click.echo(f"{red_x} {e}")
            return
        except redis.exceptions.ConnectionError as e:
            click.echo(f"{red_x} {e}")
            return

    console.print(state)
    click.echo(f"{checkmark} Printed state for instance {instance}.")


if __name__ == "__main__":
    motioncli()
