"""
This file contains expiration policies for update queues.
"""

from enum import Enum
from typing import Optional


class ExpirePolicy(Enum):
    """
    Defines the policy for expiring items in an update operation's queue.
    Each component instance has a queue for each update operation. Items in
    the queue are processed in first-in-first-out (FIFO) order, and items
    in the queue can expire based on the expiration policy set by the
    developer.

    Attributes:
        NONE: Indicates no expiration policy. Items in the queue do not expire.
        NUM_NEW_UPDATES: Items expire based on the number of new updates. Once
            the number of new updates exceeds a certain threshold, the oldest
            items are removed.
        SECONDS: Items expire based on time. Items older than a specified
            number of seconds are removed.

    Use the `expire_after` and `expire_policy` arguments in `Component.update`
    decorator to set the expiration policy for an update operation.

    Example Usage:
    ```python
    from motion import Component, ExpirePolicy

    C = Component("C")

    @C.init_state
    def setup():
        return {"default_value": 0, "some_value": 0, "another_value": 0}

    @C.update(
        "something",
        expire_after=10,
        expire_policy=ExpirePolicy.NUM_NEW_UPDATES
    )
    def update_num_new(state, props):
        # Do an expensive operation that could take a while
        ...
        return {"some_value": state["some_value"] + props["value"]}

    @C.update("something", expire_after=1, expire_policy=ExpirePolicy.SECONDS)
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

    1. The default policy is to not expire any items (ExpirePolicy.NONE), so
    the value of `default_value` will be the sum of all the values passed to
    `run` (i.e., `sum(range(100))`).

    2. The NUM_NEW_UPDATES policy will expire items in the queue once the
    number of new updates exceeds a certain threshold. The threshold is set by
    the `expire_after` argument in the `update` decorator. So the result will
    be < 4950 because the NUM_NEW_UPDATES policy will have expired some items.

    3. This will be < 4950 because the SECONDS policy will have expired some
    items (only whatever updates could have been processed in the second after
    they were added to the queue).
    """

    NONE = 0
    """ No expiration policy. """

    NUM_NEW_UPDATES = 1
    """ Expire items based on the number of new updates. """

    SECONDS = 2
    """ Expire items based on time (in seconds). """


def validate_policy(policy: ExpirePolicy, expire_after: Optional[int]) -> None:
    if policy == ExpirePolicy.NONE:
        if expire_after is not None:
            raise ValueError("expire_after must be None for policy NONE")
        return

    if expire_after is None:
        raise ValueError("expire_after must be set for policy != NONE")

    if expire_after <= 0:
        raise ValueError("expire_after must be > 0")
