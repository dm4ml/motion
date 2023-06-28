"""
This file tests the state migration functionality of motion.
"""

from motion import Component, StateMigrator

import pytest

Something = Component("Something")


@Something.init_state
def setup():
    return {"state_val": 0}


def bad_migrator(state, something_else):
    return state


def migrator_not_returning_dict(state):
    return "this isn't a dict"


def good_migrator(state):
    state.update({"another_val": 0})
    return state


def test_state_migration():
    # Create a bunch of instances
    instance_ids = []
    for _ in range(10):
        s = Something()
        instance_ids.append(s.instance_id)

    # Run bad migrators
    with pytest.raises(TypeError):
        sm = StateMigrator("helloworld", bad_migrator)

    with pytest.raises(AssertionError):
        sm = StateMigrator(Something, migrator_not_returning_dict)
        sm.migrate()

    # Run good migrator
    sm = StateMigrator(Something, good_migrator)
    result = sm.migrate([instance_ids[0]])
    assert len(result) == 1
    assert result[0].instance_id == instance_ids[0]
    assert result[0].exception is None

    # Run good migrator on all instances
    results = sm.migrate()
    assert len(results) == 10
    for result in results:
        assert result.instance_id in instance_ids
        assert result.exception is None

    # Assert the instances have the new state
    for instance_id in instance_ids:
        s = Something(instance_id)
        assert s._executor._state == {"state_val": 0, "another_val": 0}
