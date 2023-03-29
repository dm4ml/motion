import motion
import os
import random
import shutil
import pytest


@pytest.fixture
def schema():
    class TestSchema(motion.Schema):
        name: str
        age: int
        doubled_age: int

    return TestSchema


@pytest.fixture
def schema_with_liked():
    class TestSchemaWithLiked(motion.Schema):
        name: str
        age: int
        doubled_age: int
        liked: str

    return TestSchemaWithLiked


@pytest.fixture
def double_age_trigger():
    def set_doubled_age(cursor, triggered_by):
        cursor.set(
            relation=triggered_by.relation,
            identifier=triggered_by.identifier,
            key_values={"doubled_age": 2 * triggered_by.value},
        )

    return set_doubled_age


@pytest.fixture
def half_age_trigger():
    def set_half_age(cursor, triggered_by):
        cursor.set(
            relation=triggered_by.relation,
            identifier=triggered_by.identifier,
            key_values={"age": int(0.5 * triggered_by.value)},
        )

    return set_half_age


@pytest.fixture
def liked_trigger():
    def set_liked(cursor, triggered_by):
        likes = ["pizza", "ice cream", "chocolate"]

        for like in likes:
            new_id = cursor.duplicate(
                relation=triggered_by.relation,
                identifier=triggered_by.identifier,
            )
            cursor.set(
                relation=triggered_by.relation,
                identifier=new_id,
                key_values={"liked": like},
            )

    return set_liked


@pytest.fixture
def basic_config(schema, double_age_trigger):
    # Set environment variable here
    os.environ["MOTION_HOME"] = "/tmp/motion"
    config = {
        "application": {
            "name": "test1",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": {
            "test": schema,
        },
        "triggers": {
            double_age_trigger: ["test.age"],
        },
    }
    yield config


@pytest.fixture
def config_with_two_triggers(
    schema_with_liked, double_age_trigger, liked_trigger
):
    os.environ["MOTION_HOME"] = "/tmp/motion"
    config = {
        "application": {
            "name": "test2",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": {
            "test": schema_with_liked,
        },
        "triggers": {
            double_age_trigger: ["test.age"],
            liked_trigger: ["test.doubled_age"],
        },
    }
    yield config


@pytest.fixture
def config_with_multiple_triggers_on_one_key(
    schema_with_liked, double_age_trigger, liked_trigger
):
    os.environ["MOTION_HOME"] = "/tmp/motion"
    config = {
        "application": {
            "name": "test3",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": {
            "test": schema_with_liked,
        },
        "triggers": {
            double_age_trigger: ["test.age"],
            liked_trigger: ["test.age"],
        },
    }
    yield config


@pytest.fixture
def config_with_cycle(schema, double_age_trigger, half_age_trigger):
    os.environ["MOTION_HOME"] = "/tmp/motion"
    config = {
        "application": {
            "name": "test4",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": {
            "test": schema,
        },
        "triggers": {
            double_age_trigger: ["test.age"],
            half_age_trigger: ["test.doubled_age"],
        },
    }
    yield config


@pytest.fixture
def StatefulTrigger():
    class StatefulTrigger(motion.Trigger):
        def routes(self):
            return [
                motion.Route(
                    relation="test", key="age", infer=self.infer, fit=self.fit
                )
            ]

        def setUp(self, cursor):
            multiplier = 2
            return {
                "model": lambda x: x * multiplier,
                "multiplier": multiplier,
            }

        def infer(self, cursor, triggered_by):
            multiplied_value = self.state["model"](triggered_by.value)
            cursor.set(
                relation=triggered_by.relation,
                identifier=triggered_by.identifier,
                key_values={"multiplied_age": multiplied_value},
            )

        def fit(self, cursor, triggered_by):
            new_multiplier = self.state["multiplier"] + 1

            return {
                "model": lambda x: x * new_multiplier,
                "multiplier": new_multiplier,
            }

    return StatefulTrigger


@pytest.fixture
def MultipliedAges():
    class MultipliedAges(motion.Schema):
        name: str
        age: int
        multiplied_age: int

    return MultipliedAges


@pytest.fixture
def simple_stateful_config(StatefulTrigger, MultipliedAges):
    os.environ["MOTION_HOME"] = "/tmp/motion"

    config = {
        "application": {
            "name": "test5",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": {
            "test": MultipliedAges,
        },
        "triggers": {
            StatefulTrigger: ["test.age"],
        },
    }
    yield config


@pytest.fixture
def cron_trigger():
    def cron_trigger(cursor, triggered_by):
        cursor.set(
            relation="test",
            identifier="",
            key_values={"name": "Johnny", "age": random.randint(10, 30)},
        )

    return cron_trigger


@pytest.fixture
def basic_config_with_cron(schema, double_age_trigger, cron_trigger):
    # Set environment variable here
    os.environ["MOTION_HOME"] = "/tmp/motion"
    config = {
        "application": {
            "name": "test6",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": {
            "test": schema,
        },
        "triggers": {
            double_age_trigger: ["test.age"],
            cron_trigger: ["* * * * *"],
        },
    }
    yield config


@pytest.fixture
def schema_with_blob():
    class TestSchemaWithBlob(motion.Schema):
        name: str
        age: int
        doubled_age: int
        photo: bytes

    return TestSchemaWithBlob


@pytest.fixture
def basic_config_with_blob(schema_with_blob, double_age_trigger):
    # Set environment variable here
    os.environ["MOTION_HOME"] = "/tmp/motion"
    config = {
        "application": {
            "name": "test7",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": {
            "test": schema_with_blob,
        },
        "triggers": {
            double_age_trigger: ["test.age"],
        },
    }
    yield config


@pytest.fixture(scope="session", autouse=True)
def run_after_tests():
    yield

    # Cleanup: remove the temporary directory
    shutil.rmtree(
        "/tmp/motion",
    )
