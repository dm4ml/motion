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


@Test.infer("noop")
async def noop(state, value):
    return value


@Test.fit("increment")
def noop(state, values, infer_results):
    return {"count": state["count"] + 1}


app = FastAPI()


@app.get("/sync_endpoint")
def read_endpoint():
    # Create some instance of a component
    t = Test("testid")
    t.run(increment=True)
    t.flush_fit("increment")

    return {"value": t.read_state("count")}


@app.get("/async_endpoint")
async def read_noop():
    t = Test("testid")
    return {"value": await t.arun(noop=1)}


@pytest.fixture
def client():
    return TestClient(app)


def test_endpoint(client):
    response = client.get("/sync_endpoint")
    assert response.status_code == 200
    assert response.json() == {"value": 1}

    # Do it again!
    response = client.get("/sync_endpoint")
    assert response.status_code == 200
    assert response.json() == {"value": 2}

    # Try the async endpoint
    response = client.get("/async_endpoint")
    assert response.status_code == 200
    assert response.json() == {"value": 1}
