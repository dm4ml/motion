# Component Visualization Tool

We have developed a tool to visualize the structure of a Motion component. The tool is available [here](https://dm4ml.github.io/motion-vis/).

## Usage

To get a Motion component file, you should run the CLI tool in the repository with your Motion component:

```bash
$ motion vis <filename>:<component_object>
```

For example, if I had a file called `main.py` like this:

```python
from motion import Component

ZScoreComponent = Component("ZScore")


@ZScoreComponent.init_state
def setUp():
    return {"mean": None, "std": None, "values": []}


@ZScoreComponent.serve("number")
def serve(state, props):  # (1)!
    if state["mean"] is None:
        return None
    return abs(props["value"] - state["mean"]) / state["std"]


@ZScoreComponent.update("number")
def update(state, props):  # (2)!
    # Result of the serve op can be accessed via
    # props.serve_result
    # We don't do anything with the results, but we could!
    value_list = state["values"]
    value_list.append(props["value"])

    mean = sum(value_list) / len(value_list)
    std = sum((n - mean) ** 2 for n in value_list) / len(value_list)
    return {"mean": mean, "std": std, "values": value_list}
```

I would run the CLI tool like this:

```bash
$ motion vis main.py:ZScoreComponent
```

This will generate and save a JSON file to the current directory. You can then upload this file to the [vis tool](https://dm4ml.github.io/motion-vis) visualize the component.

## CLI Documentation

Running `motion vis --help` will show the following:

```bash
$ motion vis --help
Usage: motion vis [OPTIONS] FILENAME

  Visualize a component.

Options:
  --output TEXT  JSON filename to output the component graph to.
  --help         Show this message and exit.

  Example usage: motion vis main.py:MyComponent
```
