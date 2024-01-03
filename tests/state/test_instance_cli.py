from motion import Component, clear_instance, inspect_state, get_instances

import pytest
import os

C = Component("MyComponent")


@C.init_state
def setUp():
    return {"value": 0}


def test_instance_clear():
    c_instance = C()
    instance_name = c_instance.instance_name
    assert c_instance.read_state("value") == 0
    assert c_instance.get_version() == 1
    c_instance.shutdown()

    # Print all instances
    instances = get_instances(C.name)

    # Clear instance
    cleared = clear_instance(instance_name)
    assert cleared

    # Assert new state is different from old state
    new_instance = C(instance_name.strip("__")[1])
    assert new_instance.read_state("value") == 0
    assert new_instance.get_version() == 1

    # Make sure there are no cached resuts
    assert (
        len(new_instance._executor._redis_con.keys(f"MOTION_RESULT:{instance_name}"))
        == 0
    )

    new_instance.shutdown()

    # Clear something that doesn't exist
    cleared = clear_instance("DoesNotExist__somename")
    assert not cleared

    # Clear something of the wrong type
    with pytest.raises(ValueError):
        clear_instance("DoesNotExist")


def test_instance_inspect():
    c_instance = C()
    instance_name = c_instance.instance_name

    # Inspect instance
    state = inspect_state(instance_name)

    assert state == {"value": 0}


def test_list_instances():
    instance_ids = get_instances(C.name)

    assert len(instance_ids) >= 1
