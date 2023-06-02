"""This creates a mock fastapi instance and a motion
component instance within the fastapi endpoint.

Using a web app framework like fastapi runs motion
in some thread that isn't the main thread of the
main interpreter, so this is a good test to have.
"""
from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from motion import Component

Test = Component("Test")


@Test.init_state
def setup():
    return {"count": 0}


@Test.fit("increment")
def noop(state, values, infer_results):
    return {"count": state["count"] + 1}


app = FastAPI()


@app.get("/endpoint")
def read_endpoint():
    # Create some instance of a component
    t = Test("testid")
    t.run(increment=True)
    t.flush_fit("increment")

    yield {"value": t.read_state("count")}

    t.shutdown()


@pytest.fixture
def client():
    return TestClient(app)


def test_endpoint(client):
    response = client.get("/endpoint")
    assert response.status_code == 200
    assert response.json() == [{"value": 1}]

    # Do it again!
    response = client.get("/endpoint")
    assert response.status_code == 200
    assert response.json() == [{"value": 2}]
