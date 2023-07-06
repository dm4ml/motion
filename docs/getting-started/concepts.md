# Motion Concepts

Motion applications consist of _components_ that hold state and _operations_ that read and update state. Components and operations are connected by _dataflows_.

## The Component Lifecycle

When a component instance is first created, an `init` function initializes the component's state. The state is a dictionary of key-value pairs.

Components can have multiple dataflows that read and update the state. A dataflow is represented by a string _key_ and consists of two user-defined operations, which run back-to-back:

- **Infer**: a function that takes in (1) the current state dictionary and (2) any keyword arguments, then returns a result back to the user.

- **Fit**: a function that runs in the background and takes in (1) the current state dictionary, (2) the result from the infer operation, and (3) any keyword arguments. It returns any updates to the state, which can be used in future operations.

Infer operations do not modify the state, while fit operations do.

### Things to Keep in Mind

- The `infer` operation is run on the main thread, while the `fit` operation is run in the background. You directly get access to `infer` results, but `fit` results are not accessible unless you read values from the state dictionary.
- Components can only have one infer operation per key.
- Components can have many dataflows, each with their own key, infer operation, and fit operation(s).
- `Infer` results are cached, with a default expiration time of 24 hours. If you run a component twice on the same dataflow key-value pair, the second run will return the result of the first run. To override the caching behavior, see the [API docs](/motion/api/component-instance/#motion.instance.ComponentInstance.run).

## Example Component

Here is an example component that computes the z-score of a value with respect to its history.

```python title="main.py" linenums="1"
from motion import Component
import time

ZScoreComponent = Component("ZScore")


@ZScoreComponent.init_state
def setUp():
    return {"mean": None, "std": None, "values": []}


@ZScoreComponent.infer("number")
def infer(state, value):  # (1)!
    if state["mean"] is None:
        return None
    return abs(value - state["mean"]) / state["std"]


@ZScoreComponent.fit("number")
def fit(state, infer_result, value):  # (2)!
    # We don't do anything with the results, but we could!
    value_list = state["values"]
    value_list.append(value)

    mean = sum(value_list) / len(value_list)
    std = sum((n - mean) ** 2 for n in value_list) / len(value_list)
    return {"mean": mean, "std": std, "values": value_list}
```

1. This function is executed on the main thread, and the result is immediately returned to the user.
2. This function is executed in the background and merges the updates back to the state when ready.

To run the component, we can create an instance of our component, `c`, and call `c.run` on the dataflow's key and value:

```python title="main.py" linenums="28"
if __name__ == "__main__":
    c = ZScoreComponent() # Create instance of component

    # Observe 10 values of the dataflow's key
    for i in range(9):
        print(c.run("number", kwargs={"value": i}))  # (1)!

    c.run("number", kwargs={"value": 9}, flush_fit=True)  # (2)!
    for i in range(10, 19):
        print(c.run("number", kwargs={"value": i}))  # (3)!

    print(c.run("number", kwargs={"value": 10})) # (4)!
    time.sleep(5)  # Give time for the second fit to finish
    print(c.run("number", kwargs={"value": 10}, force_refresh=True))
```

1. The first few runs might return None, as the mean and std are not yet initialized.
2. This will block until the resulting fit operation has finished running. Fit ops run in the order that dataflows were executed (i.e., the fit op for number 8 will run before the fit op for number 9).
3. This uses the updated state dictionary from the previous run operation, since `flush_fit` also updates the state.
4. This uses the cached result for 10. To ignore the cached result and rerun the infer op with a (potentially old) state, we should call `c.run("number", kwargs={"value": 10}, ignore_cache=True)`. To make sure we have the latest state, we can call `c.run("number", kwargs={"value": 10}, force_refresh=True)`.

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
0.6666666666666666
0.03327787021630613
```

Note that the `fit` operation is running in a separate process, whenever new results come in. This is why the first several calls to `c.run` return `None`.

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
