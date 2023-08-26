from motion import Component
from motion.state import TempValue

import pytest
import time

TempCounter = Component("TempCounter")


def test_temp_state_value():
    counter = TempCounter()

    # Assert nothing in it
    with pytest.raises(KeyError):
        counter.read_state("value")

    # Add a temp value
    val = TempValue(0, ttl=1)
    counter.write_state({"value": val})

    # Check that it's there after clearing cache
    assert counter.read_state("value", force_refresh=True) == 0

    # Sleep for a bit
    time.sleep(1)

    # Check that it's gone after clearing cache
    with pytest.raises(KeyError):
        counter.read_state("value", force_refresh=True)
