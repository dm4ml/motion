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
        relation="Test",
        identifier=None,
        key_values={"name": "John", "age": 20},
    )
    doubled_age = cursor.get(
        relation="Test", identifier=student_id, keys=["doubled_age"]
    )["doubled_age"]

    assert doubled_age == 40

    # Checkpoint
    store.checkpoint_pa()
    store.stop()

    # Restore from checkpoint
    new_store = motion.init(basic_config, session_id=session_id)
    new_cursor = new_store.cursor()
    new_doubled_age = new_cursor.get(
        relation="Test", identifier=student_id, keys=["doubled_age"]
    )["doubled_age"]

    assert new_doubled_age == 40


def test_cron(basic_config_with_cron):
    store = motion.init(basic_config_with_cron)

    # Wait for cron trigger
    store.waitForTrigger("Cron")

    # Read doubled age
    cursor = store.cursor()
    results = cursor.sql("SELECT * FROM Test", as_df=True).to_dict("records")[
        0
    ]

    assert results["doubled_age"] == 2 * results["age"]
    assert results["name"] == "Johnny"
    assert results["session_id"] == store.session_id


def create_sample_config(relation):
    sample_config = {
        "application": {
            "name": "testmigration",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": [relation],
        "triggers": [],
    }
    return sample_config


def test_migration(entry):
    class TestRel(motion.Schema):
        key1: str

    sample_config = create_sample_config(TestRel)

    store = motion.init(sample_config)
    session_id = store.session_id

    # Add some data
    cursor = store.cursor()
    testid = cursor.set(
        relation="TestRel", identifier="", key_values={"key1": "something"}
    )
    results = cursor.get(relation="TestRel", identifier=testid, keys=["*"])
    assert results["key1"] == "something"
    store.checkpoint_pa()
    store.stop(wait=False)

    # Do a migration
    class TestRel(motion.Schema):
        key1: str
        key2: str

    sample_config_2 = create_sample_config(TestRel)
    store = motion.init(sample_config_2, session_id=session_id)

    # Check if data is still there
    results = cursor.get(relation="TestRel", identifier=testid, keys=["*"])
    print(results)
    assert False


#     ids = []
# session_id = ""
# for _ in range(2):
#     connection = motion.test(
#         MCONFIG, motion_logging_level="INFO", session_id=session_id
#     )
#     session_id = connection.session_id

#     testid = connection.set(
#         relation="TestRel",
#         identifier="",
#         key_values={"key1": "something", "key2": "somethingelse"},
#     )
#     ids.append(testid)
#     results = connection.mget(
#         relation="TestRel", identifiers=ids, keys=["*"], as_df=True
#     )
#     print(results)

#     connection.checkpoint()
