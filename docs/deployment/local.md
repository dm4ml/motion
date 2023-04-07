# Serving a Motion Application Locally


To serve a Motion application locally, run `motion serve` in your application directory (i.e., where the `mconfig.py` file is located). This will start a server that will listen for new `set` calls to relations. The server will also serve a REST API that can be used to connect to the application. The server will listen on `localhost:5000` by default, but you can specify a host and port of your choice.


```bash
$ motion serve --help

Usage: motion serve [OPTIONS] [HOST] [PORT]

  Serves a Motion application.

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
