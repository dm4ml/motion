import io
import json
import typing
from enum import Enum

import pandas as pd
import requests
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from motion.store import Store


class ClientConnection:
    """A client connection to a Motion application."""

    def __init__(
        self,
        name: str,
        server: typing.Union[str, FastAPI],
        bearer_token: str,
    ) -> None:
        if not bearer_token:
            raise ConnectionError(
                f"Could not find bearer token for {name}. Please set the MOTION_API_TOKEN environment variable, or pass in the token to the `motion.connect` function if you are using `motion.connect`."
            )

        self.name = name

        self.request_headers = {"Authorization": f"Bearer {bearer_token}"}
        if isinstance(server, FastAPI):
            self.server = server

        else:
            self.server = "http://" + server  # type: ignore
            try:
                response = requests.get(self.server + "/ping/")  # type: ignore
                if response.status_code != 200:
                    raise Exception(
                        f"Could not successfully connect to server for {self.name}; getting status code {response.status_code}."
                    )
            except requests.exceptions.ConnectionError:
                raise Exception(
                    f"Could not connect to server for {self.name} at {self.server}. Please run `motion serve` first."
                )
            self.session_id = requests.get(self.server + "/session_id/").json()  # type: ignore

    def addStore(self, store: Store) -> None:
        self.store = store
        self.session_id = self.store.session_id

    def close(self, wait: bool = False) -> None:
        if isinstance(self.server, FastAPI):
            self.store.stop(wait=wait)

    def __del__(self) -> None:
        self.close(wait=False)

    def getWrapper(self, dest: str, **kwargs: typing.Any) -> typing.Any:
        if isinstance(self.server, FastAPI):
            with TestClient(self.server) as client:
                response = client.request(
                    "get", dest, json=kwargs, headers=self.request_headers
                )
        else:
            response = requests.get(
                self.server + dest, json=kwargs, headers=self.request_headers
            )

        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code, detail=response.content
            )

        with io.BytesIO(response.content) as data:
            if response.headers["content-type"] == "application/octet-stream":
                df = pd.read_parquet(data, engine="pyarrow")
                return df

            if response.headers["content-type"] == "application/json":
                return json.loads(response.content)

    def postWrapper(
        self, dest: str, data: typing.Any, files: typing.Any = None
    ) -> typing.Any:
        if isinstance(self.server, FastAPI):
            with TestClient(self.server) as client:
                response = client.request(
                    "post",
                    dest,
                    data=data,
                    files=files,
                    headers=self.request_headers,
                )
        else:
            response = requests.post(
                self.server + dest,
                data=data,
                files=files,
                headers=self.request_headers,
            )

        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code, detail=response.content
            )

        return response.json()

    def waitForTrigger(self, trigger: str) -> typing.Any:
        """Waits for a cron-scheduled trigger to complete its first run.

        Args:
            trigger (str): The name of the trigger.
        """
        return self.postWrapper("/wait_for_trigger/", data={"trigger": trigger})

    def get(
        self,
        *,
        relation: str,
        identifier: str,
        keys: list[str],
        **kwargs: typing.Any,
    ) -> typing.Any:
        """Get values for an identifier's keys in a relation. Can pass in ["*"] as the keys argument to get all keys. Wrapper for the cursor's get method.

        Args:
            relation (str): The relation to get the value from.
            identifier (str): The identifier of the record to get the value for.
            keys (typing.List[str]): The keys to get the values for.

        Keyword Args:
            include_derived (bool, optional): Whether to include derived ids in the result. Defaults to False.
            filter_null (bool, optional): Whether to filter out null values. Filters all records with any null walue for any of the keys requested. Only used in conjuction with include_derived. Defaults to True.
            as_df (bool, optional): Whether to return the result as a pandas dataframe. Defaults to False.

        Returns:
            typing.Any: The values for the keys.
        """
        args = {
            "relation": relation,
            "identifier": identifier,
            "keys": keys,
        }
        args.update(kwargs)

        response = self.getWrapper("/get/", **args)
        if not kwargs.get("as_df", False):
            response = response.to_dict(orient="records")
            if len(response) == 1:
                response = response[0]

        return response

    def mget(
        self,
        *,
        relation: str,
        identifiers: list[str],
        keys: list[str],
        **kwargs: typing.Any,
    ) -> typing.Any:
        """Get values for a many identifiers' keys in a relation. Can pass in ["*"] as the keys argument to get all keys. Wrapper for the cursor's mget method.

        Args:
            relation (str): The relation to get the value from.
            identifiers (typing.List[int]): The ids of the records to get the value for.
            keys (typing.List[str]): The keys to get the values for.

        Keyword Args:
            include_derived (bool, optional): Whether to include derived ids in the result. Defaults to False.
            filter_null (bool, optional): Whether to filter out null values. Filters all records with any null walue for any of the keys requested. Only used in conjuction with include_derived. Defaults to True.
            as_df (bool, optional): Whether to return the result as a pandas dataframe. Defaults to False.


        Returns:
            pd.DataFrame: The values for the key.
        """
        if not isinstance(identifiers, list):
            try:
                identifiers = list(identifiers)
            except Exception:
                raise TypeError("identifiers must be a list or iterable")

        args = {
            "relation": relation,
            "identifiers": identifiers,
            "keys": keys,
        }
        args.update(kwargs)
        response = self.getWrapper("/mget/", **args)
        if not kwargs.get("as_df", False):
            return response.to_dict(orient="records")
        return response

    def set(
        self,
        *,
        relation: str,
        identifier: str,
        key_values: typing.Dict[str, typing.Any],
    ) -> typing.Any:
        """Sets given key-value pairs for an identifier in a relation.
        Overwrites existing values. Wrapper for the cursor's set method.

        Args:
            relation (str): The relation to set the value in.
            identifier (str): The id of the record to set the value for.
            key_values (typing.Dict[str, typing.Any]): The key-value pairs to set.

        Returns:
            str: The identifier of the record.
        """
        # Convert enums to their values

        for key, value in key_values.items():
            if isinstance(value, Enum):
                key_values.update({key: value.value})

        # Turn key-values into a dataframe to convert to parquet
        if identifier is None:
            identifier = ""

        df = pd.DataFrame([key_values])
        memory_buffer = io.BytesIO()
        df.to_parquet(memory_buffer, engine="pyarrow", index=False)
        memory_buffer.seek(0)

        # Create request args
        args = {"args": json.dumps({"relation": relation, "identifier": identifier})}

        return self.postWrapper(
            "/set_python/",
            data=args,
            files={
                "key_values": (
                    "key_values",
                    memory_buffer,
                    "application/octet-stream",
                )
            },
        )

    def sql(self, *, query: str, as_df: bool = True) -> typing.Any:
        """Executes a SQL query on the relations. Specify the relations as tables in the query. Wrapper for the cursor's sql method.

        Args:
            query (str): SQL query to execute.
            as_df (bool, optional): Whether to return the result as a pandas dataframe. Defaults to True.

        Returns:
            typing.Any: Pandas dataframe if as_df is True, else list of tuples.
        """
        args = {"query": query, "as_df": as_df}

        return self.getWrapper("/sql/", **args)

    def duplicate(self, *, relation: str, identifier: str) -> typing.Any:
        """Duplicates a record in a relation. Doesn't rerun any triggers for old keys on the new record. Wrapper for the cursor's duplicate method.

        Args:
            relation (str): The relation to duplicate the record in.
            identifier (str): The identifier of the record to duplicate.

        Returns:
            str: The new identifier of the duplicated record.
        """
        data = json.dumps({"relation": relation, "identifier": identifier})
        return self.postWrapper(
            "/duplicate/",
            data=data,
        )

    def checkpoint(self) -> None:
        """Checkpoints the data store to disk. Wrapper for the store's checkpoint method. Takes no arguments and returns nothing."""
        self.postWrapper("/checkpoint/", data={})
