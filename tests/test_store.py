"""
Tests the following store functions:

* checkpoint
* load from checkpoint
* add relation
* add trigger
* cron
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


def test_cron(basic_config_with_cron):
    store = motion.init(basic_config_with_cron)

    # Wait for cron trigger
    store.waitForTrigger("cron_trigger")

    # Read doubled age
    cursor = store.cursor()
    results = cursor.sql("SELECT * FROM test", as_df=True).to_dict("records")[
        0
    ]

    assert results["doubled_age"] == 2 * results["age"]
    assert results["name"] == "Johnny"
    assert results["session_id"] == store.session_id
