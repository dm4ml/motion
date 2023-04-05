# Connecting to a Motion Application

With the application all defined, we need to run it and be able to connect to it. Typically in production, we want the application constantly running, listening for new `set` calls to relations (i.e., serving). Other times, we want to connect to the application simply for testing purposes (i.e., tune hyperparameters, inspect model outputs, etc.). 

In this section, we will cover how to serve and connect to a Motion application.

## Serving a Motion Application Locally


To serve a Motion application locally, run `motion serve` in your application directory (i.e., where the `mconfig.py` file is located). This will start a server that will listen for new `set` calls to relations. The server will also serve a REST API that can be used to connect to the application. The server will listen on `localhost:5000` by default, but you can specify a host and port of your choice.


```bash
$ motion serve --help

Usage: motion serve [OPTIONS] [HOST] [PORT]

  Serves a motion application.

Options:
  -l, --logging-level TEXT  Logging level for motion. Can be DEBUG, INFO,
                            WARNING, ERROR, CRITICAL.
  --help                    Show this message and exit.
```

You can optionally specify a logging level for the server. The default is `WARNING`. The logging level `INFO` denotes all trigger starts and ends for records in any relation.

## Connecting to a Served Motion Application

Once the application is served, you can connect to it using the `motion.connect` function.

::: motion.connect
    handler: python
    options:
      heading_level: 3
      show_root_full_path: true
      show_root_toc_entry: true
      show_root_heading: true
      show_source: false

## Using the `ClientConnection` Object

The [`ClientConnection`](/api/clientconn) object returned by `motion.connect` can be used to `set` and `get` records from relations. Under the hood, the `ClientConnection` object is a wrapper around the FastAPI-generated REST API that is served by the application.

The `set` and `get` methods of the `ClientConnection` object have the same interface as the `set` and `get` methods of the `Cursor` object. You will most likely only use the following `ClientConnection` methods:

- [`set`](/api/clientconn#set)
- [`get`](/api/clientconn#get)
- [`mget`](/api/clientconn#mget)
- [`sql`](/api/clientconn#sql)

Methods like [`duplicate`](/api/clientconn#duplicate) are rarely used in a client context, since the client mainly issues `set` calls and retrieves results from `get` calls to immediately return to the user. We will see more examples of how to use the `ClientConnection` object in the tutorial sections.

## Test Connections

Oftentimes when developing a Motion application, you will want to test the application without serving it and tearing it down constantly. You can use the `motion.test` function to connect to a test server that is automatically started and torn down for you. `motion.test` returns a `ClientConnection` object.

### Session Scope

Every test connection is associated with a session. If a `session_id` is not specified, a new session is created. This is useful for testing the application from a clean slate. The `session_id` is a random string of characters, and each `ClientConnection` object has a `session_id` attribute that you can print out.

In serving mode (i.e., `motion serve`), the session is persistent, and the `session_id = "PRODUCTION"`.

::: motion.test
    handler: python
    options:
      heading_level: 3
      show_root_full_path: true
      show_root_toc_entry: true
      show_root_heading: true
      show_source: false

### Example Usage

Here is an example of how to use `motion.test` to create a test connection. Suppose we have a file called `test_single_chat.py` in our application directory with the following code:

```python title="test_single_chat.py"
from mconfig import MCONFIG
import motion

connection = motion.test(
    MCONFIG,
    wait_for_triggers=[], # No cron-scheduled triggers in our chatbot
    motion_logging_level="INFO"
)
print(f"Session ID: {connection.session_id}") # (1)!

# Must specify keywords for every arg in .set and .get
new_id = connection.set(
    relation="Query",
    identifier="",
    key_values={"prompt": "What color is the sky?"},
)
prompts_and_completions = connection.get(
    relation="Query",
    identifier=new_id,
    keys=["prompt", "llm_completion"],
    include_derived=True, # (2)!
    as_df=True,
)
print(f"Response: {prompts_and_completions}")

connection.checkpoint() # (3)!
```

1. The `session_id` will be a random string of characters. In another call to `motion.test`, we can set `session_id` to the same value to keep the same session.
2. We want to include the many LLM completions that were generated for a single prompt, so we set `include_derived=True`.
3. This will save the prompts and completions added in this session to disk, in case we want to reinitialize the session later. Otherwise, checkpointing is only done at the interval specified in the `MCONFIG`.

## HTTP Connection

A motion application can also be connected to using HTTP requests; however, request data must be JSON-serializable. This is useful for connecting to a motion application in a different language (e.g., Javascript UI).

Check out the [HTTP API](/deployment/http) documentation for more information.