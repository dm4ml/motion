from motion import Component

import pytest
import time
import random
import numpy as np
import copy

FragmentedState = Component("FragmentedState")
UnifiedState = Component("UnifiedState")

NUM_KEYS = 1000
VECTOR_LEN = 10000
D = {
    str(i): np.array([random.random() for _ in range(VECTOR_LEN)])
    for i in range(NUM_KEYS)
}
print("Done generating keys")


@FragmentedState.init_state
def setupf():
    # Make a bunch of keys and values
    return D


@UnifiedState.init_state
def setupu():
    d = copy.deepcopy(D)
    return {"state": d}


def test_key_level_serialization_faster():
    fs = FragmentedState()
    print("Done initializing FragmentedState")
    us = UnifiedState()
    print("Done initializing UnifiedState")

    # Time how long it takes to read a key
    start = time.time()
    fs.read_state("0")
    end = time.time()
    key_level_time = end - start

    # Time how long it takes to serialize the same dict and read it
    start = time.time()
    us.read_state("state")["0"]
    end = time.time()
    unified_level_time = end - start

    assert key_level_time < unified_level_time
