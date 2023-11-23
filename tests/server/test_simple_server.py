import pytest
from fastapi.testclient import TestClient

from motion import Component
from motion.server.application import Application

# Create some components

Counter = Component("Counter")


@Counter.init_state
def setup():
    return {"multiplier": 1}


@Counter.serve("sum")
def compute_sum(state, props):
    return sum(props["values"]) * state["multiplier"]


@Counter.update("sum")
def update_sum(state, props):
    return {"multiplier": state["multiplier"] + 1}


@pytest.fixture
def client():
    # Create application
    motion_app = Application(components=[Counter])
    credentials = motion_app.get_credentials()
    app = motion_app.get_app()

    return credentials, TestClient(app)


def test_endpoint(client):
    credentials, client = client  # Unpack

    # Test the run endpoint
    response = client.post(
        "/Counter",
        json={
            "instance_id": "testid",
            "dataflow_key": "sum",
            "async_action": False,
            "props": {"values": [1, 2, 3]},
        },
        headers={"Authorization": f"Bearer {credentials['secret_token']}"},
    )

    assert response.status_code == 200
    assert response.json() == 6

    # Read the state and check that it was updated
    response = client.get(
        "/Counter/read",
        params={
            "instance_id": "testid",
            "key": "multiplier",
        },
        headers={"Authorization": f"Bearer {credentials['secret_token']}"},
    )

    assert response.status_code == 200
    assert response.json() == {"multiplier": 2}
