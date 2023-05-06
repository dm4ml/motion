from typing import Any, Dict
from motion import Component

import sqlite3

from sklearn.datasets import make_regression
from sklearn.linear_model import LinearRegression


class DBComponent(Component):
    def setUp(self):
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

    @Component.infer("count")
    def execute_fn(self, state, value):
        return state["cursor"].execute("SELECT COUNT(*) FROM users").fetchall()

    @Component.infer("something")
    def noop(self, state, value):
        return state["fit_count"]

    @Component.fit("something")
    def increment(self, state, values, infer_results):
        return {"fit_count": state["fit_count"] + 1}


def test_db_component():
    c = DBComponent()
    assert c.run(count=1) == [(2,)]
    c.run(something=1, wait_for_fit=True)
    assert c.run(something=1) == 1
