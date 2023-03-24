# motion

A framework for building ML applications, using a trigger-based execution model.

## Getting Started

You must use Python 3.7. We recommend using poetry to manage your virtual environment and dependencies. You can install poetry [here](https://python-poetry.org/docs/). Once you have poetry installed, clone this repo and run the following command to install the dependencies:

```bash
poetry shell
poetry install
```

## Using API

Run `motion serve` to start the API server from your application directory.

Writing a get request:

```
js

const identifier = await fetch('http://localhost:8000/js/set/', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({
    "relation": "query",
    "key_values": {"prompt": "the beach"}
  }),
});
```

This should return an identifier, like "cb1f1cd1-67d3-40b5-88a1-25e0ba1b0663".

You can then use this identifier to get the result:

```
js

const getresponse = await fetch('http://localhost:8000/js/get/', {
  method: 'GET',
  headers: {
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({
    "relation": "query",
    "identifier": identifier,
    "keys": [
        "identifier",
        "text_suggestion",
        "catalog_img_id",
        "catalog_img_score",
    ],
    "include_derived": true
  }),
});
```