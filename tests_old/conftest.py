import motion
import os
import random
import shutil
import pytest


@pytest.fixture(scope="session", autouse=True)
def entry():
    os.environ["MOTION_HOME"] = "/tmp/motion"
    os.environ["MOTION_API_TOKEN"] = "test_api_token"

    yield

    # Cleanup: remove the temporary directory
    shutil.rmtree(
        "/tmp/motion",
    )


@pytest.fixture
def schema(entry):
    class Test(motion.Schema):
        name: str
        age: int
        doubled_age: int

    return Test


@pytest.fixture
def schema_with_liked(entry):
    class Test(motion.Schema):
        name: str
        age: int
        doubled_age: int
        liked: str

    return Test


@pytest.fixture
def double_age_trigger(entry):
    class DoubleAge(motion.Trigger):
        def routes(self):
            return [
                motion.Route(relation="Test", key="age", infer=self.infer, fit=None)
            ]

        def setUp(self, cursor):
            return {}

        def infer(self, cursor, trigger_context):
            cursor.set(
                relation=trigger_context.relation,
                identifier=trigger_context.identifier,
                key_values={"doubled_age": int(2 * trigger_context.value)},
            )

    return DoubleAge


@pytest.fixture
def half_age_trigger(entry):
    class HalfAge(motion.Trigger):
        def routes(self):
            return [
                motion.Route(
                    relation="Test",
                    key="doubled_age",
                    infer=self.infer,
                    fit=None,
                )
            ]

        def setUp(self, cursor):
            return {}

        def infer(self, cursor, trigger_context):
            cursor.set(
                relation=trigger_context.relation,
                identifier=trigger_context.identifier,
                key_values={"age": int(0.5 * trigger_context.value)},
            )

    return HalfAge


@pytest.fixture
def liked_trigger(entry):
    class Liked(motion.Trigger):
        def routes(self):
            return [
                motion.Route(
                    relation="Test",
                    key="doubled_age",
                    infer=self.infer,
                    fit=None,
                )
            ]

        def setUp(self, cursor):
            return {}

        def infer(self, cursor, trigger_context):
            likes = ["pizza", "ice cream", "chocolate"]

            for like in likes:
                new_id = cursor.duplicate(
                    relation=trigger_context.relation,
                    identifier=trigger_context.identifier,
                )
                cursor.set(
                    relation=trigger_context.relation,
                    identifier=new_id,
                    key_values={"liked": like},
                )

    return Liked


@pytest.fixture
def basic_config(schema, double_age_trigger):
    # Set environment variable here
    # os.environ["MOTION_HOME"] = "/tmp/motion"
    config = {
        "application": {
            "name": "test1",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": [schema],
        "triggers": [double_age_trigger],
    }
    yield config


@pytest.fixture
def config_with_two_triggers(schema_with_liked, double_age_trigger, liked_trigger):
    # os.environ["MOTION_HOME"] = "/tmp/motion"
    config = {
        "application": {
            "name": "test2",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": [schema_with_liked],
        "triggers": [double_age_trigger, liked_trigger],
    }
    yield config


@pytest.fixture
def config_with_multiple_triggers_on_one_key(
    schema_with_liked, double_age_trigger, liked_trigger
):
    # os.environ["MOTION_HOME"] = "/tmp/motion"
    liked_trigger.routes = lambda self: [
        motion.Route(relation="Test", key="age", infer=self.infer, fit=None)
    ]
    config = {
        "application": {
            "name": "test3",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": [schema_with_liked],
        "triggers": [double_age_trigger, liked_trigger],
    }
    yield config


@pytest.fixture
def config_with_cycle(schema, double_age_trigger, half_age_trigger):
    # os.environ["MOTION_HOME"] = "/tmp/motion"
    config = {
        "application": {
            "name": "test4",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": [schema],
        "triggers": [double_age_trigger, half_age_trigger],
    }
    yield config


@pytest.fixture
def StatefulTrigger(entry):
    class StatefulTrigger(motion.Trigger):
        def routes(self):
            return [
                motion.Route(
                    relation="MultipliedAges",
                    key="age",
                    infer=self.infer,
                    fit=self.fit,
                )
            ]

        def setUp(self, cursor):
            multiplier = 2
            return {
                "model": lambda x: x * multiplier,
                "multiplier": multiplier,
            }

        def infer(self, cursor, trigger_context):
            multiplied_value = self.state["model"](trigger_context.value)
            cursor.set(
                relation=trigger_context.relation,
                identifier=trigger_context.identifier,
                key_values={"multiplied_age": multiplied_value},
            )

        def fit(self, cursor, trigger_context, infer_context):
            new_multiplier = self.state["multiplier"] + 1

            return {
                "model": lambda x: x * new_multiplier,
                "multiplier": new_multiplier,
            }

    return StatefulTrigger


@pytest.fixture
def MultipliedAges(entry):
    class MultipliedAges(motion.Schema):
        name: str
        age: int
        multiplied_age: int

    return MultipliedAges


@pytest.fixture
def simple_stateful_config(StatefulTrigger, MultipliedAges):
    # os.environ["MOTION_HOME"] = "/tmp/motion"

    config = {
        "application": {
            "name": "test5",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": [MultipliedAges],
        "triggers": [StatefulTrigger],
    }
    yield config


@pytest.fixture
def cron_trigger(entry):
    class Cron(motion.Trigger):
        def routes(self):
            return [
                motion.Route(
                    relation="",
                    key="* * * * *",
                    infer=self.infer,
                    fit=None,
                )
            ]

        def setUp(self, cursor):
            return {}

        def infer(self, cursor, trigger_context):
            cursor.set(
                relation="Test",
                identifier="",
                key_values={"name": "Johnny", "age": random.randint(10, 30)},
            )

    return Cron


@pytest.fixture
def basic_config_with_cron(schema, double_age_trigger, cron_trigger):
    # Set environment variable here
    # os.environ["MOTION_HOME"] = "/tmp/motion"
    config = {
        "application": {
            "name": "test6",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": [schema],
        "triggers": [double_age_trigger, cron_trigger],
    }
    yield config


@pytest.fixture
def schema_with_blob(entry):
    class TestSchemaWithBlob(motion.Schema):
        name: str
        age: int
        doubled_age: int
        photo: bytes

    return TestSchemaWithBlob


@pytest.fixture
def basic_config_with_blob(schema_with_blob, double_age_trigger):
    # Set environment variable here
    # os.environ["MOTION_HOME"] = "/tmp/motion"

    double_age_trigger.routes = lambda self: [
        motion.Route(
            relation="TestSchemaWithBlob",
            key="age",
            infer=self.infer,
            fit=None,
        )
    ]

    config = {
        "application": {
            "name": "test7",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": [schema_with_blob],
        "triggers": [double_age_trigger],
    }
    yield config
