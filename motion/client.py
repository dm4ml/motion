import pandas as pd
import requests

from enum import Enum
from motion.store import Store

import io
import json
import typing


from fastapi.testclient import TestClient
from fastapi import FastAPI


class ClientConnection(object):
    """A client connection to a motion store.

    Args:
        name (str): The name of the store.
    """

    def __init__(
        self,
        name: str,
        server: typing.Union[str, FastAPI],
    ) -> None:
        self.name = name

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

    def close(self, wait: bool = True) -> None:
        if isinstance(self.server, FastAPI):
            self.store.stop(wait=wait)

    def __del__(self) -> None:
        self.close(wait=False)

    def getWrapper(self, dest: str, **kwargs: typing.Any) -> typing.Any:
        if isinstance(self.server, FastAPI):
            with TestClient(self.server) as client:
                response = client.request("get", dest, json=kwargs)
        else:
            response = requests.get(self.server + dest, json=kwargs)

        if response.status_code != 200:
            raise Exception(response.content)

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
                response = client.request("post", dest, data=data, files=files)
        else:
            response = requests.post(
                self.server + dest, data=data, files=files
            )

        if response.status_code != 200:
            raise Exception(response.content)

        return response.json()

    def waitForTrigger(self, trigger: str) -> typing.Any:
        """Wait for a trigger to fire.

        Args:
            trigger (str): The name of the trigger.
        """
        return self.postWrapper(
            "/wait_for_trigger/", data={"trigger": trigger}
        )

    def get(self, **kwargs: typing.Any) -> typing.Any:
        response = self.getWrapper("/get/", **kwargs)
        if not kwargs.get("as_df", False):
            return response.to_dict(orient="records")
        return response

    def mget(self, **kwargs: typing.Any) -> typing.Any:
        response = self.getWrapper("/mget/", **kwargs)
        if not kwargs.get("as_df", False):
            return response.to_dict(orient="records")
        return response

    def set(self, **kwargs: typing.Any) -> typing.Any:
        # Convert enums to their values
        for key, value in kwargs["key_values"].items():
            if isinstance(value, Enum):
                kwargs["key_values"].update({key: value.value})

        args = {
            "args": json.dumps(
                {k: v for k, v in kwargs.items() if k != "key_values"}
            )
        }

        # Turn key-values into a dataframe
        df = pd.DataFrame(kwargs["key_values"], index=[0])

        # Convert to parquet stream
        memory_buffer = io.BytesIO()
        df.to_parquet(memory_buffer, engine="pyarrow", index=False)
        memory_buffer.seek(0)

        return self.postWrapper(
            "/set_python/",
            data=args,
            files={
                "file": (
                    "key_values",
                    memory_buffer,
                    "application/octet-stream",
                )
            },
        )

    def sql(self, **kwargs: typing.Any) -> typing.Any:
        return self.getWrapper("/sql/", **kwargs)
