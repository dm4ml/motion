"""
Tests the following trigger functions:

* state retrieval
* state update
* setup and fit return values
* bad route objects
* key not found in state

* multiple triggers in one set
* cycle of triggers

"""

import motion
import os
import pytest


def test_state_retrieval(simple_stateful_config):
    store = motion.init(simple_stateful_config)
    cursor = store.cursor()

    identifier = cursor.set(
        relation="MultipliedAges",
        identifier=None,
        key_values={"name": "John", "age": 15},
    )

    full_record = cursor.get(
        relation="MultipliedAges",
        identifier=identifier,
        keys=["*"],
        include_derived=True,
    )[0]

    assert full_record["name"] == "John"
    assert full_record["age"] == 15
    assert full_record["multiplied_age"] == 30


def test_state_update(simple_stateful_config):
    store = motion.init(simple_stateful_config)
    cursor = store.cursor()

    identifiers = []
    for _ in range(3):
        identifier = cursor.set(
            relation="MultipliedAges",
            identifier=None,
            key_values={"name": "John", "age": 15},
        )
        cursor.waitForResults()  # Wait for fit to run
        identifiers.append(identifier)

    all_records = cursor.mget(
        relation="MultipliedAges",
        identifiers=identifiers,
        keys=["*"],
        include_derived=True,
        as_df=True,
    )

    for i, row in all_records.iterrows():
        assert row["name"] == "John"
        assert row["age"] == 15
        assert row["multiplied_age"] == (i + 2) * row["age"]


@pytest.fixture
def ImproperFit():
    class ImproperFit(motion.Trigger):
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

        def fit(self, cursor, trigger_context):
            return 1

    return ImproperFit


def test_improper_fit(entry, MultipliedAges, ImproperFit):
    # Use a fit function that returns a value that is not a dict
    # os.environ["MOTION_HOME"] = "/tmp/motion"

    config = {
        "application": {
            "name": "test_improper_fit",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": [MultipliedAges],
        "triggers": [ImproperFit],
    }

    store = motion.init(config)
    cursor = store.cursor()

    def filter_warning(record):
        return (
            "fit() of trigger ImproperFit should return a dict of state updates"
            in str(record.message)
        )

    with pytest.warns(None, match=filter_warning):
        cursor.set(
            relation="MultipliedAges",
            identifier="",
            key_values={"name": "John", "age": 15, "multiplied_age": 30},
        )


@pytest.fixture
def ImproperSetup():
    class ImproperSetup(motion.Trigger):
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
            return "hello world"

        def infer(self, cursor, trigger_context):
            multiplied_value = self.state["model"](trigger_context.value)
            cursor.set(
                relation=trigger_context.relation,
                identifier=trigger_context.identifier,
                key_values={"multiplied_age": multiplied_value},
            )

        def fit(self, cursor, trigger_context):
            return {}

    return ImproperSetup


def test_improper_setup(entry, MultipliedAges, ImproperSetup):
    # os.environ["MOTION_HOME"] = "/tmp/motion"

    config = {
        "application": {
            "name": "test_improper_setup",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": [MultipliedAges],
        "triggers": [ImproperSetup],
    }

    with pytest.raises(TypeError):
        store = motion.init(config)


def test_route_errors(entry, StatefulTrigger, MultipliedAges):
    # os.environ["MOTION_HOME"] = "/tmp/motion"

    config = {
        "application": {
            "name": "test_bad_routes",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": [MultipliedAges],
        "triggers": [StatefulTrigger],
    }

    store = motion.init(config)
    cursor = store.cursor()

    # Try wrong type
    StatefulTrigger.routes = lambda self: "hello world!"

    with pytest.raises(TypeError):
        store = motion.init(config)

    # Try specifying no infer or fit, this should pass
    StatefulTrigger.routes = lambda self: [
        motion.Route(relation="MultipliedAges", key="age")
    ]
    store = motion.init(config)
    cursor = store.cursor()
    cursor.set(
        relation="MultipliedAges",
        identifier=None,
        key_values={"name": "John", "age": 15, "multiplied_age": 30},
    )


@pytest.fixture
def KeyNotFoundInState():
    class KeyNotFoundInState(motion.Trigger):
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
            return {}

        def infer(self, cursor, trigger_context):
            multiplied_value = self.state["model"](trigger_context.value)
            cursor.set(
                relation=trigger_context.relation,
                identifier=trigger_context.identifier,
                key_values={"multiplied_age": multiplied_value},
            )

        def fit(self, cursor, trigger_context):
            return {}

    return KeyNotFoundInState


def test_key_not_found_in_state(entry, KeyNotFoundInState, MultipliedAges):
    # os.environ["MOTION_HOME"] = "/tmp/motion"

    config = {
        "application": {
            "name": "test_key_not_found_in_state",
            "author": "shreyashankar",
            "version": "0.1",
        },
        "relations": [MultipliedAges],
        "triggers": [KeyNotFoundInState],
    }

    store = motion.init(config)
    cursor = store.cursor()

    with pytest.raises(KeyError):
        cursor.set(
            relation="MultipliedAges",
            identifier=None,
            key_values={"name": "John", "age": 15, "multiplied_age": 30},
        )


def test_multiple_triggers_in_one_set(
    config_with_multiple_triggers_on_one_key,
):
    store = motion.init(config_with_multiple_triggers_on_one_key)
    cursor = store.cursor()

    identifier = cursor.set(
        relation="Test",
        identifier=None,
        key_values={"name": "John", "age": 15},
    )
    results = cursor.get(
        relation="Test",
        identifier=identifier,
        keys=["*"],
        include_derived=True,
        as_df=True,
    )

    assert len(results) == 3

    for like in ["pizza", "ice cream", "chocolate"]:
        assert like in results["liked"].values

    for doubled_age in results["doubled_age"].values:
        assert doubled_age == 30


def test_cycle(config_with_cycle):
    store = motion.init(config_with_cycle)
    cursor = store.cursor()

    with pytest.raises(RecursionError):
        cursor.set(
            relation="Test",
            identifier=None,
            key_values={"name": "John", "age": 15},
        )
