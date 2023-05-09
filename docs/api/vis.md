# Component Visualization Tool

We have developed a tool to visualize the structure of a Motion component. The tool is available [here](https://dm4ml.github.io/motion-vis/).

## Usage

To get a Motion component file, you should run the CLI tool in the repository with your Motion component:

```bash
$ motion vis <filename>::<component_object>
```

For example, if I had a file called `main.py` like this:

```python
from motion import Component

c = Component("Z-Score")


@c.init
def setUp():
    return {"mean": None, "std": None}


@c.infer("number")
def normalize(state, value):
    if state["mean"] is None:
        return None
    return abs(value - state["mean"]) / state["std"]


@c.fit("number", batch_size=10)
def update(state, values, infer_results):
    # We don't do anything with the results, but we could!
    mean = sum(values) / len(values)
    std = sum((n - mean) ** 2 for n in values) / len(values)
    return {"mean": mean, "std": std}


if __name__ == "__main__":
    # Observe 10 values of the dataflow's key
    for i in range(9):
        print(c.run(number=i))

    c.run(number=9, wait_for_fit=True)
    for i in range(10, 19):
        print(c.run(number=i))
```

I would run the CLI tool like this:

```bash
$ motion vis main.py::c
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
```
