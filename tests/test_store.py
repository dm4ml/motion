"""
Tests the following store functions:

* checkpoint
* load from checkpoint
* add relation
* add trigger
"""
import motion
import os
import pytest


def test_checkpoint(basic_config):
    store = motion.init(basic_config)
    session_id = store.session_id

    # Add some data
    cursor = store.cursor()
    student_id = cursor.set(
        relation="test",
        identifier=None,
        key_values={"name": "John", "age": 20},
    )
    doubled_age = cursor.get(
        relation="test", identifier=student_id, keys=["doubled_age"]
    )["doubled_age"]

    assert doubled_age == 40

    # Checkpoint
    store.checkpoint_pa()
    store.stop()

    # Restore from checkpoint
    new_store = motion.init(basic_config, session_id=session_id)
    new_cursor = new_store.cursor()
    new_doubled_age = new_cursor.get(
        relation="test", identifier=student_id, keys=["doubled_age"]
    )["doubled_age"]

    assert new_doubled_age == 40
