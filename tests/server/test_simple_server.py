import pytest
from fastapi.testclient import TestClient

from motion import Component
from motion import Application

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
    credentials, app_client = client  # Unpack

    # Get a token for the instance id
    instance_id = "testid"
    response = app_client.post(
        "/auth",
        json={
            "instance_id": instance_id,
        },
        headers={"X-Api-Key": credentials["api_key"]},
    )
    assert response.status_code == 200
    jwt_token = response.json()["token"]

    # Test the run endpoint
    response = app_client.post(
        "/Counter",
        json={
            "instance_id": instance_id,
            "flow_key": "sum",
            "is_async": False,
            "props": {"values": [1, 2, 3]},
        },
        headers={
            "Authorization": f"Bearer {jwt_token}",
            "X-Api-Key": credentials["api_key"],
        },
    )

    assert response.status_code == 200
    assert response.json() == 6

    # Read the state and check that it was updated
    response = app_client.get(
        "/Counter/read",
        params={
            "instance_id": instance_id,
            "key": "multiplier",
        },
        headers={
            "Authorization": f"Bearer {jwt_token}",
            "X-Api-Key": credentials["api_key"],
        },
    )

    assert response.status_code == 200
    assert response.json() == {"multiplier": 2}
