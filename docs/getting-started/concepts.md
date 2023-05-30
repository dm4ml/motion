# Motion Concepts

Motion applications consist of _components_ that hold state and _operations_ that update state. Components and operations are connected by _dataflows_.

## The Component Lifecycle

When a component is created, an `init` function initializes the component's state. The state is a dictionary of key-value pairs.

Components can have multiple dataflows that read and update the state. A dataflow is represented by a string _key_ and consists of two user-defined operations:

- **Infer**: a function that takes in (1) the current state dictionary and (2) the current value of the dataflow's key and returns a result.

- **Fit**: a function that takes in (1) the current state dictionary, (2) a list of recent values of the dataflow's key, and (3) a list of recent results of the `infer` operation and returns an updated state dictionary.

Infer operations do not modify the state dictionary, while fit operations do.

### Things to Keep in Mind

- The `infer` operation is run on the main thread, while the `fit` operation is run in the background. This means that the `infer` operation should be fast and not block the main thread.
- Fit operations are initialized with a batch size, which determines how many values and results are passed to the fit operation at a time.
- Components can only have one infer operation per key.
- Components can have many dataflows, each with their own key, infer operation, and fit operation(s).

## Example Component

Here is an example component that computes the z-score of a value with respect to its batched history.

```python title="main.py" linenums="1"
from motion import Component

ZScoreComponent = Component("ZScore")


@ZScoreComponent.init_state
def setUp():  # (1)!
    return {"mean": None, "std": None}


@ZScoreComponent.infer("number")
def infer(state, value):  # (2)!
    if state["mean"] is None:
        return None
    return abs(value - state["mean"]) / state["std"]


@ZScoreComponent.fit("number", batch_size=10)
def fit(state, values, infer_results):  # (3)!
    # We don't do anything with the results, but we could!
    mean = sum(values) / len(values)
    std = sum((n - mean) ** 2 for n in values) / len(values)
    return {"mean": mean, "std": std}
```

1. Must take zero arguments and return a dictionary with
   string keys and values of any type, representing initial state. This function is executed only at runtime (i.e., the first call to `c.run()`).
2. Must take two arguments: the current state dictionary and the current value of the dataflow's key. This function is executed on the main thread.
3. Must take three arguments: the current state dictionary, a list of recent values of the dataflow's key, and a list of recent results of the `infer` operation. This function is executed in the background, only when `batch_size` values have been passed to the `infer` operation.

To run the component, we can create an instance of our component, `c`, and call `c.run` on the dataflow's key and value:

```python title="main.py" linenums="24"
if __name__ == "__main__":
    c = ZScoreComponent() # Create instance of component

    # Observe 10 values of the dataflow's key
    for i in range(9):
        print(c.run(number=i))  # (1)!

    c.run(number=9, force_fit=True)  # (2)!
    for i in range(10, 19):
        print(c.run(number=i))  # (3)!
```

1. This will return None, as the state's mean and std are not yet initialized.
2. This will block until the resulting fit operation has finished running. Don't call `force_fit` if the `batch_size` has not been reached, otherwise the program will hang.
3. This uses the updated state dictionary from the previous run operation.

The output of the above code is:

```bash
> python main.py
None
None
None
None
None
None
None
None
None
0.6666666666666666
0.7878787878787878
0.9090909090909091
1.0303030303030303
1.1515151515151516
1.2727272727272727
1.393939393939394
1.5151515151515151
1.6363636363636365
```

Note that the `fit` operation is not called until the `batch_size` is reached. This is why the first 9 calls to `c.run` return `None`. In the above example, the `fit` operation is only called once, as only 19 values were observed.

## Component Parameters

You can inject static component parameters into your dataflow operations by passing them to the component constructor:

```python
from motion import Component

ZScoreComponent = Component("ZScore", params={"alert_threshold": 2.0})
```

Then, you can access the parameters in your operations:

```python
@ZScoreComponent.infer("number")
def infer(state, value):
    if state["mean"] is None:
        return None
    z_score = abs(value - state["mean"]) / state["std"]
    if z_score > ZScoreComponent.params["alert_threshold"]:
        print("Alert!")
    return z_score
```

The `params` dictionary is immutable, so you can't modify it in your operations. This functionality is useful for experimenting with different values of a parameter without having to modify your code.
