"""
Tests the following cursor functions:

* get
* set
* mget
* duplicate
* sql
* exists

"""
import motion
import pytest


def test_empty(basic_config):
    store = motion.init(basic_config)
    new_cursor = store.cursor()

    res = new_cursor.get(relation="Test", identifier="hehehehe", keys=["doubled_age"])
    assert not res


def test_get_failures(basic_config):
    store = motion.init(basic_config)

    # Add some data
    cursor = store.cursor()
    student_id = cursor.set(
        relation="Test",
        identifier=None,
        key_values={"name": "John", "age": 20},
    )

    with pytest.raises(Exception):
        cursor.get(relation="Test", identifier=student_id, keys=["heheheh"])

    with pytest.raises(Exception):
        cursor.get(relation="Test", identifier=student_id, keys=[])

    with pytest.raises(Exception):
        cursor.get(relation="Test", identifier=student_id, keys=None)


def test_get_all(basic_config):
    store = motion.init(basic_config)

    # Add some data
    cursor = store.cursor()
    student_id = cursor.set(
        relation="Test",
        identifier=None,
        key_values={"name": "John", "age": 20},
    )

    results = cursor.get(relation="Test", identifier=student_id, keys=["*"])
    assert results["name"] == "John"
    assert results["age"] == 20
    assert results["doubled_age"] == 40


def test_set_failures(basic_config):
    store = motion.init(basic_config)
    cursor = store.cursor()

    # Add data in malformed ways
    with pytest.raises(TypeError):
        cursor.set(relation="heheheh")

    with pytest.raises(KeyError):
        cursor.set(
            relation="heheheh",
            identifier=None,
            key_values={"name": "John", "age": 20},
        )

    with pytest.raises(AttributeError):
        cursor.set(
            relation="Test",
            identifier=None,
            key_values={"name": "John", "lllage": 20},
        )

    with pytest.raises(TypeError):
        cursor.set(
            relation="Test",
            identifier=None,
            key_values={"name": "John", "age": "20"},
        )


def test_duplicate_set(basic_config):
    store = motion.init(basic_config)
    cursor = store.cursor()

    identifier = cursor.set(
        relation="Test",
        identifier=None,
        key_values={"name": "John", "age": 15},
    )
    first_age = cursor.get(
        relation="Test", identifier=identifier, keys=["age", "doubled_age"]
    )
    assert first_age["age"] == 15
    assert first_age["doubled_age"] == 30

    identifier = cursor.set(
        relation="Test",
        identifier=identifier,
        key_values={"age": 25},
    )
    second_age = cursor.get(
        relation="Test", identifier=identifier, keys=["age", "doubled_age"]
    )
    assert second_age["age"] == 25
    assert second_age["doubled_age"] == 50


def test_include_derived_get(config_with_two_triggers):
    store = motion.init(config_with_two_triggers)
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


def test_mget(config_with_two_triggers):
    store = motion.init(config_with_two_triggers)
    cursor = store.cursor()

    identifiers = []
    names = ["John", "Jane", "Joe", "Jill", "Jack"]
    ages = [15, 16, 17, 18, 19]
    liked = ["pizza", "ice cream", "chocolate"]

    for name, age in zip(names, ages):
        identifier = cursor.set(
            relation="Test",
            identifier=None,
            key_values={"name": name, "age": age},
        )
        identifiers.append(identifier)

    result_df = cursor.mget(
        relation="Test",
        identifiers=identifiers,
        keys=["*"],
        include_derived=True,
        as_df=True,
    )

    assert len(result_df) == len(names) * len(liked)

    for _, row in result_df.iterrows():
        assert row["name"] in names
        assert row["age"] in ages
        assert row["liked"] in liked
        assert row["doubled_age"] == row["age"] * 2

    # Test mget without returning as df
    results = cursor.mget(
        relation="Test",
        identifiers=identifiers,
        keys=["*"],
        include_derived=True,
        as_df=False,
    )
    assert len(results) == len(names) * len(liked)
    assert type(results[0]) == dict


def test_duplicate_id(basic_config):
    store = motion.init(basic_config)
    cursor = store.cursor()

    identifier = cursor.set(
        relation="Test",
        identifier=None,
        key_values={"name": "John", "age": 15},
    )

    for _ in range(3):
        new_id = cursor.duplicate(relation="Test", identifier=identifier)
        cursor.set(
            relation="Test",
            identifier=new_id,
            key_values={"age": 16},
        )

    results = cursor.get(
        relation="Test",
        identifier=identifier,
        keys=["*"],
        as_df=True,
        include_derived=True,
    )

    assert len(results) == 4
    for _, row in results.iterrows():
        assert row["age"] * 2 == row["doubled_age"]


def test_sql(basic_config):
    store = motion.init(basic_config)
    cursor = store.cursor()

    cursor.set(
        relation="Test",
        identifier=None,
        key_values={"name": "John", "age": 15},
    )

    results = cursor.sql(
        "SELECT * FROM Test WHERE name = 'John'",
        as_df=True,
    )

    assert len(results) == 1
    assert results["name"].values[0] == "John"
    assert results["age"].values[0] == 15
    assert results["doubled_age"].values[0] == 30

    results = cursor.sql("SELECT * FROM Test WHERE name = 'John'", as_df=False)
    assert len(results) == 1


def test_exists(basic_config):
    store = motion.init(basic_config)
    cursor = store.cursor()

    identifier = cursor.set(
        relation="Test",
        identifier=None,
        key_values={"name": "John", "age": 15},
    )

    assert cursor.exists(relation="Test", identifier=identifier)
    assert not cursor.exists(relation="Test", identifier="heheheh")

    with pytest.raises(KeyError):
        cursor.exists(relation="heheheh", identifier=identifier)
