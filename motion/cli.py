import importlib
import json
from datetime import datetime

import click


@click.group()
def motioncli() -> None:
    """Motion commands."""
    pass


@motioncli.command("vis")
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
    if "::" not in filename:
        click.echo("Component must be in the format `filename::componentinstance`.")
        return

    # Remove the file extension if present
    module = filename.replace(".py", "")

    module, instance = module.split("::")
    if not module or not instance:
        click.echo("Component must be in the format `module::componentinstance`.")
        return

    module = importlib.import_module(module)  # type: ignore

    # Get the class instance
    class_instance = getattr(module, instance)

    # Get the graph
    graph = class_instance.get_graph()

    # Dump the graph to a file with the date
    ts = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    out_filename = f"{ts}_{instance}_{output}"

    with open(output, "w") as f:
        json.dump(graph, f, indent=4)
        click.echo(f"Graph dumped to {out_filename}.")
