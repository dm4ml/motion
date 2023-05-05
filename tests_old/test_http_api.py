"""
Tests the following api functions:

* HTTP API: get, mget, set, sql, wait_for_trigger, session_id

"""

import motion
import os
import pandas as pd
import pytest
import random

from fastapi.testclient import TestClient


@pytest.fixture
def test_http_client(config_with_two_triggers):
    # Create store and client

    store = motion.init(config_with_two_triggers, session_id="HTTP_TESTING")
    app = motion.api.create_fastapi_app(store, testing=True)

    yield app

    # Close connection
    store.stop()


@pytest.fixture
def test_create_headers(entry):
    bearer_token = os.environ["MOTION_API_TOKEN"]
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}",
    }


def test_http_set_get(test_http_client, test_create_headers):
    with TestClient(test_http_client) as client:
        set_response = client.request(
            "post",
            "/json/set/",
            json={
                "relation": "Test",
                "identifier": "",
                "key_values": {"name": "Mary", "age": random.randint(10, 30)},
            },
            headers=test_create_headers,
        )

        identifier = set_response.json()

        get_response = client.request(
            "get",
            "/json/get/",
            json={
                "relation": "Test",
                "identifier": identifier,
                "keys": ["*"],
                "include_derived": True,
            },
            headers=test_create_headers,
        )
        results = get_response.json()
        results_df = pd.DataFrame(results)

        assert len(results_df) == 3
        assert results_df["name"].iloc[0] == "Mary"
        assert (results_df["doubled_age"] == results_df["age"] * 2).all()


def test_http_mget(test_http_client, test_create_headers):
    with TestClient(test_http_client) as client:
        # Create some data
        identifiers = []
        for i in range(10):
            set_response = client.request(
                "post",
                "/json/set/",
                json={
                    "relation": "Test",
                    "identifier": "",
                    "key_values": {
                        "name": "John",
                        "age": random.randint(10, 30),
                    },
                },
                headers=test_create_headers,
            )
            identifiers.append(set_response.json())

        # Get data
        mget_response = client.request(
            "get",
            "/json/mget/",
            json={
                "relation": "Test",
                "identifiers": identifiers,
                "keys": ["*"],
                "include_derived": True,
            },
            headers=test_create_headers,
        )
        results = mget_response.json()
        results_df = pd.DataFrame(results)

        assert len(results_df) == 30
        assert results_df["name"].iloc[0] == "John"
        assert (results_df["doubled_age"] == results_df["age"] * 2).all()


def test_http_sql(test_http_client, test_create_headers):
    with TestClient(test_http_client) as client:
        # Create some data
        set_response = client.request(
            "post",
            "/json/set/",
            json={
                "relation": "Test",
                "identifier": "",
                "key_values": {
                    "name": "John",
                    "age": random.randint(10, 30),
                },
            },
            headers=test_create_headers,
        )
        identifier = set_response.json()

        # Get data. Note that we use the SQL API here, so
        # there will be no value for the "liked" key.
        sql_response = client.request(
            "get",
            "/json/sql/",
            json={
                "query": f"SELECT * FROM Test WHERE identifier = '{identifier}'",
            },
            headers=test_create_headers,
        )
        results = sql_response.json()
        results_df = pd.DataFrame(results)

        assert len(results_df) == 1
        assert results_df["name"].iloc[0] == "John"
        assert (results_df["doubled_age"] == results_df["age"] * 2).all()


def test_http_utils(test_http_client, test_create_headers):
    with TestClient(test_http_client) as client:
        # No cron triggers in this one
        response = client.request(
            "post",
            "/json/wait_for_trigger/",
            json={"trigger": "trigger_1"},
            headers=test_create_headers,
        )

        assert response.status_code == 500

        # Test session id

        response = client.request(
            "get", "/json/session_id/", headers=test_create_headers
        )
        assert response.status_code == 200
        assert response.json() == "HTTP_TESTING"


def test_wait_for_cron_triggers(basic_config_with_cron, test_create_headers):
    # Create store and client

    store = motion.init(basic_config_with_cron, session_id="HTTP_TESTING")
    app = motion.api.create_fastapi_app(store, testing=True)

    with TestClient(app) as client:
        response = client.request(
            "post",
            "/json/wait_for_trigger/",
            json={"trigger": "Cron"},
            headers=test_create_headers,
        )

        assert response.status_code == 200
        assert response.json() == "Cron"

    # Stop store
    store.stop()
