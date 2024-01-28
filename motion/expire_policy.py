"""
This file contains expiration policies for update queues.
"""

from enum import Enum


class ExpirePolicy(Enum):
    NONE = 0
    NUM_NEW_UPDATES = 1
    SECONDS = 2


def validate_policy(policy: ExpirePolicy, expire_after: int) -> None:
    if policy == ExpirePolicy.NONE:
        if expire_after is not None:
            raise ValueError("expire_after must be None for policy NONE")
        return

    if expire_after is None:
        raise ValueError("expire_after must be set for policy != NONE")

    if expire_after <= 0:
        raise ValueError("expire_after must be > 0")
