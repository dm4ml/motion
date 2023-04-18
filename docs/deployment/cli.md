# Command Line Interface Usage

Motion comes with a command line interface (CLI). We list the available commands below.

## `motion create`

The `motion create` command creates a new Motion application. It takes the name of the application and the author name as arguments. The application will be created in the current directory.

```bash
$ motion create --help
Usage: motion create [OPTIONS]

  Creates a new application.

Options:
  --name TEXT    The name of your application.
  --author TEXT  Author name.
  --help         Show this message and exit.
```

## `motion serve`

The `motion serve` command serves a Motion application. It takes the host and port as arguments. The default host is `localhost` and the default port is `5000`.

```bash
$ motion serve --help
Usage: motion serve [OPTIONS]

  Serves a Motion application.

Options:
  --name TEXT               Project name.
  --host TEXT               Host to serve on.
  --port INTEGER            Port to serve on.
  -l, --logging-level TEXT  Logging level for motion. Can be DEBUG, INFO,
                            WARNING, ERROR, CRITICAL.
  --help                    Show this message and exit.
```

## `motion clear`

The `motion clear` command clears the database of a Motion application. It takes the application name as an argument.

```bash
$ motion clear --help
Usage: motion clear [OPTIONS] NAME

  Removes the data store for the given Motion application.

Options:
  --help  Show this message and exit.
```

## `motion token`

The `motion token` command generates a token for a Motion application. It takes no arguments. We recommend you use this command to generate a token for your application and store it in an environment variable. You can then use this token to connect to your application from a [`ClientConnection`](/api/clientconn/).

```bash
$ motion token --help
Usage: motion token [OPTIONS]

  Generate a new API token

Options:
  --help  Show this message and exit.
```

## `motion example`

The `motion example` command creates a new Motion application from the repository of example projects. It takes the name of the example application and the author name as arguments. The application will be created in the current directory.

```bash
$ motion example --help
Usage: motion example [OPTIONS]

  Creates a new application from an example.

Options:
  --name TEXT    One of the example applications to create. Can be: 'cooking'.
  --author TEXT  Author name.
  --help         Show this message and exit.
```

Note that you can only create an example application from the list of available examples (i.e., `cooking`).

## `motion logs`

The `motion logs` command prints the logs of a Motion application. It takes the application name as an argument, and optional `session_id` and `limit` arguments.

```bash
$ motion logs --help
Usage: motion logs [OPTIONS] NAME

  Show logs for a Motion application

Options:
  --session-id TEXT  Session ID to show logs for.
  --limit INTEGER    Limit number of logs to show.
  --help             Show this message and exit.
```