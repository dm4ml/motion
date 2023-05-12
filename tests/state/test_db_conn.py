from motion import Component

import sqlite3


def test_db_component():
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

    @c.infer("count")
    def execute_fn(state, value):
        return state["cursor"].execute("SELECT COUNT(*) FROM users").fetchall()

    @c.infer("something")
    def noop(state, value):
        return state["fit_count"]

    @c.fit("something")
    def increment(state, values, infer_results):
        return {"fit_count": state["fit_count"] + 1}

    c_instance = c()
    assert c_instance.run(count=1) == [(2,)]
    c_instance.run(something=1, wait_for_fit=True)
    assert c_instance.run(something=1) == 1
