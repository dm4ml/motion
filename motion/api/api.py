from fastapi import (
    FastAPI,
    HTTPException,
    Request,
    Response,
    status,
    Form,
    File,
    UploadFile,
    Body,
    APIRouter,
)
from fastapi.exceptions import RequestValidationError
from io import BytesIO
from pydantic import ValidationError
from urllib.parse import parse_qs

import binascii
import logging
import pandas as pd
import pyarrow as pa

from pydantic import BaseModel, Extra, Json
from motion.api.models import *


def df_to_json_response(df):
    # response = pa.serialize(df).to_buffer()
    return Response(
        df.to_parquet(engine="pyarrow", index=False),
        media_type="application/octet-stream",
    )


def create_app(store, testing=False):
    app = FastAPI()

    json_app = FastAPI(
        title="Motion JSON API",
        description="An API for calling basic Motion functions on JSON data.",
        # version="1.0.0",
        # servers=[{"url": "https://your-app-url.com"}],
    )
    app.mount("/json", json_app)

    @app.on_event("startup")
    async def startup():
        app.state.testing = testing
        app.state.store = store

    @app.get("/get/")
    async def get(args: GetRequest):
        cur = app.state.store.cursor()

        res = cur.get(**args.kwargs)

        # Check if the result is a pandas df
        if not isinstance(res, pd.DataFrame):
            res = pd.DataFrame(res)

        return df_to_json_response(res)

    @app.get("/mget/")
    async def mget(args: MgetRequest):
        cur = app.state.store.cursor()
        res = cur.mget(
            **args.kwargs,
        )
        if not isinstance(res, pd.DataFrame):
            res = pd.DataFrame(res)

        return df_to_json_response(res)

    @app.post("/set_python/")
    async def set_python(
        args: Json[PartialSetRequest], file: UploadFile = File(...)
    ):
        args = args.dict()
        content = await file.read()
        with BytesIO(content) as f:
            if file.content_type == "application/octet-stream":
                df = pd.read_parquet(f, engine="pyarrow")
                args["key_values"] = df.to_dict(orient="records")[0]

        cur = app.state.store.cursor()
        return cur.set(
            relation=args["relation"],
            identifier=args["identifier"],
            key_values=args["key_values"],
        )

    @app.get("/sql/")
    async def sql(args: SqlRequest):
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

    @app.get("/session_id/")
    async def session_id():
        return app.state.store.session_id

    @app.on_event("shutdown")
    async def shutdown():
        if not app.state.testing:
            app.state.store.stop()

    # JSON API

    @json_app.post("/set/")
    async def json_set(args: SetRequest):
        cur = app.state.store.cursor()
        try:
            identifier = cur.set(
                relation=args.relation,
                identifier=args.identifier,
                key_values=args.key_values,
            )
            return Response(identifier, media_type="application/json")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @json_app.get("/get/")
    async def json_get(args: GetRequest):
        cur = app.state.store.cursor()

        try:
            res = cur.get(**args.kwargs)
            return res
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @json_app.get("/mget/")
    async def json_mget(args: MgetRequest):
        cur = app.state.store.cursor()
        try:
            res = cur.mget(**args.kwargs)
            return res
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @json_app.get("/sql/")
    async def json_sql(args: SqlRequest):
        cur = app.state.store.cursor()
        try:
            res = cur.sql(args.query, as_df=False)
            return res
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @json_app.get("/session_id/")
    async def json_session_id():
        return app.state.store.session_id

    return app
