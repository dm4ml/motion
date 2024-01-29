"""
This file contains discard policies for update queues.
"""

from enum import Enum
from typing import Optional


class DiscardPolicy(Enum):
    """
    Defines the policy for discarding items in an update operation's queue.
    Each component instance has a queue for each update operation. Items in
    the queue are processed in first-in-first-out (FIFO) order, and items
    in the queue can delete based on the discard policy set by the
    developer.

    Attributes:
        NONE: Indicates no discard policy. Items in the queue do not delete.
        NUM_NEW_UPDATES: Items delete based on the number of new updates. Once
            the number of new updates exceeds a certain threshold, the oldest
            items are removed.
        SECONDS: Items delete based on time. Items older than a specified
            number of seconds at the time of processing are removed.

    Use the `discard_after` and `discard_policy` arguments in `Component.update`
    decorator to set the discard policy for an update operation.

    Example Usage:
    ```python
    from motion import Component, DiscardPolicy

    C = Component("C")

    @C.init_state
    def setup():
        return {"default_value": 0, "some_value": 0, "another_value": 0}

    @C.update(
        "something",
        discard_after=10,
        discard_policy=DiscardPolicy.NUM_NEW_UPDATES
    )
    def update_num_new(state, props):
        # Do an expensive operation that could take a while
        ...
        return {"some_value": state["some_value"] + props["value"]}

    @C.update("something", discard_after=1, discard_policy=DiscardPolicy.SECONDS)
    def update_seconds(state, props):
        # Do an expensive operation that could take a while
        ...
        return {"another_value": state["another_value"] + props["value"]}

    @C.update("something")
    def update_default(state, props):
        # Do an expensive operation that could take a while
        ...
        return {"default_value": state["default_value"] + props["value"]}

    if __name__ == "__main__":
        c = C()

        # If we do many runs of "something", the update queue will grow
        # and the policy will be automatically enforced by Motion.

        for i in range(100):
            c.run("something", props={"value": str(i)})

        # Flush the update queue (i.e., wait for all updates to finish)
        c.flush_update("something")

        print(c.read_state("default_value")) # (1)!

        print(c.read_state("some_value")) # (2)!

        print(c.read_state("another_value")) # (3)!

        c.shutdown()
    ```

    1. The default policy is to not delete any items (DiscardPolicy.NONE), so
    the value of `default_value` will be the sum of all the values passed to
    `run` (i.e., `sum(range(100))`).

    2. The NUM_NEW_UPDATES policy will delete items in the queue once the
    number of new updates exceeds a certain threshold. The threshold is set by
    the `discard_after` argument in the `update` decorator. So the result will
    be < 4950 because the NUM_NEW_UPDATES policy will have deleted some items.

    3. This will be < 4950 because the SECONDS policy will have deleted some
    items (only whatever updates could have been processed in the second after
    they were added to the queue).
    """

    NONE = 0
    """ No discard policy. Does not discard items in the queue. """

    NUM_NEW_UPDATES = 1
    """ Delete items based on the number of new updates enqueued. """

    SECONDS = 2
    """ Delete items based on time (in seconds). """


def validate_policy(policy: DiscardPolicy, discard_after: Optional[int]) -> None:
    if policy == DiscardPolicy.NONE:
        if discard_after is not None:
            raise ValueError("discard_after must be None for policy NONE")
        return

    if discard_after is None:
        raise ValueError("discard_after must be set for policy != NONE")

    if discard_after <= 0:
        raise ValueError("discard_after must be > 0")
