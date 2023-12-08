# This file tests the StateValue functionality

from motion import Component

import sqlite3
import os

c = Component("DBComponent")


@c.init_state
def setUp():
    # Create in-memory sqlite database
    path = ":file::memory:?cache=shared:"
    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    # Drop table if exists
    cursor.execute("DROP TABLE IF EXISTS users")

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS users
            (id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER)"""
    )

    cursor.execute("INSERT INTO users (name, age) VALUES (?, ?)", ("John Doe", 25))
    cursor.execute("INSERT INTO users (name, age) VALUES (?, ?)", ("Jane Smith", 30))
    conn.commit()

    return {"path": path, "fit_count": 0}


@c.serve("count")
def execute_fn(state, props):
    conn = sqlite3.connect(state["path"])
    cursor = conn.cursor()
    return cursor.execute("SELECT COUNT(*) FROM users").fetchall()


@c.serve("something")
def noop(state, props):
    return state["fit_count"]


@c.update("something")
def increment(state, props):
    return {"fit_count": state["fit_count"] + 1}


def test_db_component():
    c_instance = c()
    assert c_instance.run("count", props={"value": 1}, flush_update=True) == [(2,)]
    c_instance.run("something", props={"value": 1}, flush_update=True)
    assert c_instance.run("something", props={"value": 5}) == 1

    # Delete the database
    os.remove(c_instance.read_state("path"))
