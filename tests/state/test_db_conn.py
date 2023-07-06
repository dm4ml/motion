from motion import Component

import sqlite3

c = Component("DBComponent")


@c.init_state
def setUp():
    # Create in-memory sqlite database
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS users
            (id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER)"""
    )

    cursor.execute(
        "INSERT INTO users (name, age) VALUES (?, ?)", ("John Doe", 25)
    )
    cursor.execute(
        "INSERT INTO users (name, age) VALUES (?, ?)", ("Jane Smith", 30)
    )
    conn.commit()

    return {"cursor": cursor, "fit_count": 0}


@c.save_state
def save(state):
    return {"fit_count": state["fit_count"]}


@c.load_state
def load(state):
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    return {"cursor": cursor, "fit_count": state["fit_count"]}


@c.serve("count")
def execute_fn(state, value):
    return state["cursor"].execute("SELECT COUNT(*) FROM users").fetchall()


@c.serve("something")
def noop(state, value):
    return state["fit_count"]


@c.update("something")
def increment(state, value, serve_result):
    return {"fit_count": state["fit_count"] + 1}


def test_db_component():
    c_instance = c()
    assert c_instance.run("count", kwargs={"value": 1}) == [(2,)]
    c_instance.run("something", kwargs={"value": 1}, flush_update=True)
    assert c_instance.run("something", kwargs={"value": 5}) == 1
