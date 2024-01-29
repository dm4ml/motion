# Motion Concepts

Motion applications consist of _components_ that hold state and _operations_ that read and update state. Components and operations are connected by _flows_. Think of a component as representing the prompt and LLM pipeline(s) for a particular task, the state as the prompt sub-parts, and flows as the different ways to interact with the state (e.g., assemble the sub-parts into a prompt, update the sub-parts, etc).

## The Component Lifecycle

When a component instance is first created, an `init` function initializes the component's state. The state is a dictionary of key-value pairs, representing the initial sub-parts you might want to include in your prompt. The state is persisted in a key-value store (Redis) and is loaded when the component instance is initialized again.

Components can have multiple flows that read and update the state (i.e., prompt sub-parts). A flow is represented by a string _key_ and consists of two user-defined operations, which run back-to-back:

- **serve**: a function that takes in (1) a state dictionary that may not reflect all new information yet, and (2) a user-defined ` props` dictionary (passed in at runtime), then returns a result back to the user.

- **update**: a function that runs in the background and takes in (1) the current state dictionary, and (2) the user-defined `props` dictionary, including the result of the serve op (accessed via `props.serve_result`). The `update` operation returns any updates to the state, which can be used in future operations. The `props` dictionary dies after the update operation for a flow. We run update operations in the background because they may be expensive and we don't want to block the serves.

Serve operations do not modify the state, while update operations do.

## Concurrency and Consistency in Motion's Execution Engine

Since serve operations do not modify the state, you can run multiple serve operations for the same component instance in parallel (e.g., in different Python processes). However, since update operations modify the state, Motion ensures that only one update operation is running at a time for a given component instance. This is done by maintaining queues of pending update operations and issuing exclusive write locks to update operations. Each component instance has its own lock and has a queue for each of its update operations. While update operations are running, serve operations can still run with low latency using stale state. The update queue is processed in a FIFO manner.

### Backpressure in Processing Update Operations

Motion's execution engine experiences backpressure if a queue of pending update operations grows faster than the rate at which its update operations are completed. For example, if an update operation calls an LLM for a long prompt and takes 10 seconds to complete, and new update operations are being added to the queue every second, the queue will grow by 10 operations every second. While this does not pose problems for serve operations because serve operations can read stale state, it can cause the component instance to fall behind in processing update operations.

Our solution to limit queue growth is to offer a configurable `DiscardPolicy` parameter for each update operation. There are two options for `DiscardPolicy`:

- `DiscardPolicy.SECONDS`: If more than `discard_after` seconds have passed since the update operation _u_ was added to the queue, _u_ is removed from the queue and the state is not updated with _u_'s results.
- `DiscardPolicy.NUM_NEW_UPDATES`: If more than `discard_after` new update operations have been added to the queue since an update operation _u_ was added, _u_ is removed from the queue and the state is not updated with _u_'s results.

See the [API docs](/motion/api/component/#motion.DiscardPolicy) for how to use `DiscardPolicy`.

## State vs Props

The difference between state and props can be a little confusing, since both are dictionaries. The main difference is that state is persistent, while props are ephemeral/limited to a flow.

State is initialized when the component is created and persists between successive flows. Since Motion is backed by Redis, state also persists when the component is restarted. State is available to all operations for all flows, but can only be changed by update operations.

On the other hand, props are passed in at runtime and are only available to the serve and update operations for a _single_ flow. Props can be modified in serve operation, so they can be used to pass data between serve and update operations. Of note is `props.serve_result`, which is the result of the serve operation for a flow (and thus only accessible in update operations). This is useful for update operations that need to use the result of the serve operation. Think of props like a kwargs dictionary that becomes irrelevant after the particular flow is finished.

### Things to Keep in Mind

- Components can have many flows, each with their own key, serve operation, and update operation(s).
- Components can only have one serve operation per key.
- The `serve` operation is run on the main thread, while the `update` operation is run in the background. You directly get access to `serve` results, but `update` results are not accessible unless you read values from the state dictionary.
- `serve` results are cached, with a default discard time of 24 hours. If you run a component twice on the same flow key-value pair, the second run will return the result of the first run. To override the caching behavior, see the [API docs](/motion/api/component-instance/#motion.instance.ComponentInstance.run).
- `update` operations are processed sequentially in first-in-first-out (FIFO) order. This allows state to be updated incrementally. To handle backpressure, update operations can be configured to expire after a certain amount of time or after a certain number of new update operations have been added to the queue. See the [API docs](/motion/api/component/#motion.DiscardPolicy) for how to use `DiscardPolicy`.

## Example Component

Here is an example component that computes the z-score of a value with respect to its history.

```python title="main.py" linenums="1"
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

1. This function is executed on the main thread, and the result is immediately returned to the user.
2. This function is executed in the background and merges the updates back to the state when ready.

To run the component, we can create an instance of our component, `c`, and call `c.run` on the flow's key and value:

```python title="main.py" linenums="29"
if __name__ == "__main__":
    import time
    c = ZScoreComponent() # Create instance of component

    # Observe 10 values of the flow's key
    for i in range(9):
        print(c.run("number", props={"value": i}))  # (1)!

    c.run("number", props={"value": 9}, flush_update=True)  # (2)!
    for i in range(10, 19):
        print(c.run("number", props={"value": i}))  # (3)!

    print(c.run("number", props={"value": 10})) # (4)!
    time.sleep(5)  # Give time for the second update to finish
    print(c.run("number", props={"value": 10}, force_refresh=True))
```

1. The first few runs might return None, as the mean and std are not yet initialized.
2. This will block until the resulting update operation has finished running. update ops run in the order that flows were executed (i.e., the update op for number 8 will run before the update op for number 9).
3. This uses the updated state dictionary from the previous run operation, since `flush_update` also updates the state.
4. This uses the cached result for 10. To ignore the cached result and rerun the serve op with a (potentially old) state, we should call `c.run("number", props={"value": 10}, ignore_cache=True)`. To make sure we have the latest state, we can call `c.run("number", props={"value": 10}, force_refresh=True)`.

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

Note that the `update` operation is running in a separate process, whenever new results come in. This is why the first several calls to `c.run` return `None`.

## Component Parameters

You can inject static component parameters into your flow operations by passing them to the component constructor:

```python
from motion import Component

ZScoreComponent = Component("ZScore", params={"alert_threshold": 2.0})
```

Then, you can access the parameters in your operations:

```python
@ZScoreComponent.serve("number")
def serve(state, props):
    if state["mean"] is None:
        return None
    z_score = abs(props["value"] - state["mean"]) / state["std"]
    if z_score > ZScoreComponent.params["alert_threshold"]:
        print("Alert!")
    return z_score
```

The `params` dictionary is immutable, so you can't modify it in your operations. This functionality is useful for experimenting with different values of a parameter without having to modify your code.
