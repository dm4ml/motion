# HTTP Documentation

A motion application can also be connected to using HTTP requests; however, request data must be JSON-serializable. This is useful for connecting to a motion application in a different language (e.g., Javascript UI).

The main endpoints and example usages are listed in this section.

## Endpoints

### Set data

| Attribute | Value |
| --------- | ----- |
| URL | `/json/set/` |
| Method | `POST` |
| Content-Type | `application/json` |
| Authorization | `Bearer <your_motion_api_token>` |
| Payload |  <ul><li>`relation (str)`: The relation name</li><li>`identifier (str, optional)`: The identifier for the item to set. Defaults to an empty string</li> <li>`key_values (dict)`: The key-value pairs to set</li> </ul>  |

#### Example usage

=== "HTTP"

    ```bash
    POST /json/set/ HTTP/1.1
    Host: your_api_url
    Content-Type: application/json
    Authorization: Bearer your_motion_api_token

    {
        "relation": "Test",
        "identifier": "",
        "key_values": {
            "name": "Mary",
            "age": 25
        }
    }
    ```
=== "Javascript"

    ```javascript
    console.log("Hello world")
    ```

### Get data

| Attribute | Value |
| --------- | ----- |
| URL | `/json/get/` |
| Method | `GET` |
| Content-Type | `application/json` |
| Authorization | `Bearer <your_motion_api_token>` |
| Payload |  <ul><li>`relation (str)`: The relation name</li><li>`identifier (str)`: The identifier for the item to get</li> <li>`keys (list)`: The keys to get</li> <li>`include_derived (bool)`: Whether to include derived identifiers in the result</li>  </ul>  |


#### Example usage

=== "HTTP"
    ```bash
    GET /json/get/ HTTP/1.1
    Host: your_api_url
    Content-Type: application/json
    Authorization: Bearer your_motion_api_token

    {
        "relation": "Test",
        "identifier": "abc",
        "keys": ["*"],
        "include_derived": true
    }
    ```

### Get data for multiple identifiers

| Attribute | Value |
| --------- | ----- |
| URL | `/json/mget/` |
| Method | `GET` |
| Content-Type | `application/json` |
| Authorization | `Bearer <your_motion_api_token>` |
| Payload |  <ul><li>`relation (str)`: The relation name</li><li>`identifiers (str)`: The identifier for the item to get</li> <li>`keys (list)`: The keys to get</li> <li>`include_derived (bool)`: Whether to include derived identifiers in the result</li> </ul>  |


#### Example usage

=== "HTTP"
    ```bash
    GET /json/mget/ HTTP/1.1
    Host: your_api_url
    Content-Type: application/json
    Authorization: Bearer your_motion_api_token

    {
        "relation": "Test",
        "identifiers": ["abc", "def"],
        "keys": ["*"],
        "include_derived": true
        }
    ```

### Execute SQL Queries

| Attribute | Value |
| --------- | ----- |
| URL | `/json/sql/` |
| Method | `GET` |
| Content-Type | `application/json` |
| Authorization | `Bearer <your_motion_api_token>` |
| Payload |  <ul><li>`query (str)`: The SQL query to execute</li></ul> |

#### Example usage

=== "HTTP"
    ```bash
    GET /json/sql/ HTTP/1.1
    Host: your_api_url
    Content-Type: application/json
    Authorization: Bearer your_motion_api_token

    {
        "query": "SELECT * FROM Test"
    }
    ```

### Get Session ID

| Attribute | Value |
| --------- | ----- |
| URL | `/json/session_id/` |
| Method | `GET` |
| Content-Type | `application/json` |
| Authorization | `Bearer <your_motion_api_token>` |

#### Example usage

=== "HTTP"
    ```bash
    GET /json/session_id/ HTTP/1.1
    Host: your_api_url
    Content-Type: application/json
    Authorization: Bearer your_motion_api_token
    ```

### Wait for Trigger

| Attribute | Value |
| --------- | ----- |
| URL | `/json/wait_for_trigger/` |
| Method | `POST` |
| Content-Type | `application/json` |
| Authorization | `Bearer <your_motion_api_token>` |
| Payload | <ul><li>`trigger (str)`: The name of the trigger to wait for</li> </ul>  |

#### Example usage

=== "HTTP"
    ```bash
    POST /json/wait_for_trigger/ HTTP/1.1
    Host: your_api_url
    Content-Type: application/json
    Authorization: Bearer your_motion_api_token

    {
        "trigger": "TestTrigger"
    }
    ```