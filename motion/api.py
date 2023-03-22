from fastapi import (
    FastAPI,
    Request,
    Response,
    status,
    Form,
    File,
    UploadFile,
    Body,
)
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from urllib.parse import parse_qs

import binascii
import logging
import pandas as pd
import pyarrow as pa

from pydantic import BaseModel, Extra


class MotionGet(BaseModel, extra=Extra.allow):
    namespace: str
    identifier: str
    keys: list

    @property
    def kwargs(self):
        return self.__dict__


class MotionMget(BaseModel):
    namespace: str
    identifiers: list
    keys: list
    kwargs: dict = {}

    @property
    def kwargs(self):
        return self.__dict__


class MotionSet(BaseModel):
    namespace: str
    identifier: str = None
    key_values: dict
    run_duplicate_triggers: bool = False


class MotionGetNewId(BaseModel):
    namespace: str
    key: str = "identifier"


class MotionSql(BaseModel):
    query: str
    as_df: bool = True


def df_to_json_response(df):
    # response = pa.serialize(df).to_buffer()
    return Response(
        df.to_parquet(engine="pyarrow", index=False),
        media_type="application/octet-stream",
    )


def create_app(store, testing=False):
    app = FastAPI()

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ):
        detail = exc.errors()[0]["msg"]
        return Response(
            {"detail": detail},
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            media_type="application/json",
        )

    @app.on_event("startup")
    async def startup():
        app.state.testing = testing
        app.state.store = store

    @app.get("/get/")
    async def get(args: MotionGet):
        cur = app.state.store.cursor()

        res = cur.get(**args.kwargs)

        # Check if the result is a pandas df
        if not isinstance(res, pd.DataFrame):
            res = pd.DataFrame(res)

        return df_to_json_response(res)

    @app.get("/mget/")
    async def mget(args: MotionMget):
        cur = app.state.store.cursor()
        res = cur.mget(
            **args.kwargs,
        )
        if not isinstance(res, pd.DataFrame):
            res = pd.DataFrame(res)

        return df_to_json_response(res)

    @app.post("/set/")
    async def set(request: Request):
        data = await request.body()
        parsed_args = {
            k: v[0] for k, v in parse_qs(data.decode("unicode_escape")).items()
        }
        if "identifier" not in parsed_args:
            parsed_args["identifier"] = None

        top_level_args = ["namespace", "identifier", "run_duplicate_triggers"]
        args = {k: v for k, v in parsed_args.items() if k in top_level_args}
        args["key_values"] = {
            k: v for k, v in parsed_args.items() if k not in top_level_args
        }

        args = MotionSet(**args)

        cur = app.state.store.cursor()
        return cur.set(
            args.namespace,
            args.identifier,
            args.key_values,
            args.run_duplicate_triggers,
        )

    @app.get("/get_new_id/")
    async def get_new_id(args: MotionGetNewId):
        cur = app.state.store.cursor()
        return cur.getNewId(args.namespace, args.key)

    @app.get("/sql/")
    async def sql(args: MotionSql):
        cur = app.state.store.cursor()
        res = cur.sql(args.query, args.as_df)
        if isinstance(res, pd.DataFrame):
            return df_to_json_response(res)
        else:
            return Response(res, media_type="application/json")

    @app.post("/wait_for_trigger/")
    async def wait_for_trigger(request: Request):
        data = await request.body()
        trigger = parse_qs(data.decode("unicode_escape"))["trigger"][0]

        app.state.store.waitForTrigger(trigger)
        return trigger

    @app.get("/ping/")
    async def root():
        return {"message": "Hello World"}

    @app.on_event("shutdown")
    async def shutdown():
        if not app.state.testing:
            app.state.store.stop()

    return app
