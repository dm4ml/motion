import pytest
from fastapi.testclient import TestClient

from motion.dashboard import dashboard_app


@pytest.fixture
def client():
    # Create application

    return TestClient(dashboard_app)


def test_dashboard(client):
    # make sure a get request to / returns a 200
    response = client.get("/")
    assert response.status_code == 200
