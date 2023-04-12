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
import random
import pytest


def test_checkpoint(basic_config):
    store = motion.init(basic_config, session_id="test_checkpoint")
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
    log_table = motion.get_logs(
        basic_config["application"]["name"], session_id
    )

    assert len(log_table) == 1
    assert log_table["trigger_version"].values[0] == 0
    assert log_table["trigger_name"].values[0] == "DoubleAge"
    assert log_table["trigger_action_type"].values[0] == "INFER"
    assert log_table["trigger_action"].values[0] == "infer"

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


@pytest.fixture
def cron_trigger_duplicate_route(entry):
    class Cron(motion.Trigger):
        def routes(self):
            return [
                motion.Route(
                    relation="",
                    key="* * * * *",
                    infer=self.infer,
                    fit=None,
                ),
                motion.Route(
                    relation="",
                    key="* * * * *",
                    infer=self.infer2,
                    fit=None,
                ),
            ]

        def setUp(self, cursor):
            return {}

        def infer2(self, cursor, trigger_context):
            cursor.set(
                relation="Test",
                identifier="",
                key_values={"name": "Vicky", "age": random.randint(10, 30)},
            )

        def infer(self, cursor, trigger_context):
            cursor.set(
                relation="Test",
                identifier="",
                key_values={"name": "Johnny", "age": random.randint(10, 30)},
            )

    return Cron


def test_duplicate_cron_triggers(
    schema, double_age_trigger, cron_trigger_duplicate_route
):
    config = {
        "application": {
            "name": "test_duplicate_cron",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": [schema],
        "triggers": [double_age_trigger, cron_trigger_duplicate_route],
    }

    # Duplicate cron triggers should raise an error
    with pytest.raises(ValueError):
        store = motion.init(config)

    # Cron triggers with different keys should raise an error
    cron_trigger_duplicate_route.routes = lambda self: [
        motion.Route(
            relation="",
            key="0 * * * *",
            infer=self.infer,
            fit=None,
        ),
        motion.Route(
            relation="",
            key="1 * * * *",
            infer=self.infer2,
            fit=None,
        ),
    ]

    config2 = {
        "application": {
            "name": "test_duplicate_cron_2",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": [schema],
        "triggers": [double_age_trigger, cron_trigger_duplicate_route],
    }
    with pytest.raises(ValueError):
        store = motion.init(config2)


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
    cursor.close()

    # Do a migration
    class TestRel(motion.Schema):
        key1: str
        key2: str

    sample_config_2 = create_sample_config(TestRel)
    store = motion.init(sample_config_2, session_id=session_id)

    # Check if data is still there
    cursor = store.cursor()
    results = cursor.get(relation="TestRel", identifier=testid, keys=["*"])
    assert results["key1"] == "something"
    assert results["key2"] == None

    # Add key2
    cursor.set(
        relation="TestRel",
        identifier=testid,
        key_values={"key2": "somethingelse"},
    )
    results = cursor.get(relation="TestRel", identifier=testid, keys=["key2"])
    assert results["key2"] == "somethingelse"
    store.checkpoint_pa()
    store.stop(wait=False)
    cursor.close()

    # Reread and check
    sample_config_2 = create_sample_config(TestRel)
    store = motion.init(sample_config_2, session_id=session_id)
    cursor = store.cursor()
    results = cursor.get(relation="TestRel", identifier=testid, keys=["*"])
    assert results["key1"] == "something"
    assert results["key2"] == "somethingelse"
