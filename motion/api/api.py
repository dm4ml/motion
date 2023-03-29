from __future__ import annotations

import typing
from io import BytesIO
from urllib.parse import parse_qs

import pandas as pd
from fastapi import FastAPI, File, HTTPException, Request, Response, UploadFile
from pydantic import Json

from motion.api.models import *
from motion.store import Store


def df_to_json_response(df: pd.DataFrame) -> Response:
    return Response(
        df.to_parquet(engine="pyarrow", index=False),
        media_type="application/octet-stream",
    )


def create_app(store: Store, testing: bool = False) -> FastAPI:
    app = FastAPI()

    json_app = FastAPI(
        title="Motion JSON API",
        description="An API for calling basic Motion functions on JSON data.",
        # version="1.0.0",
        # servers=[{"url": "https://your-app-url.com"}],
    )
    app.mount("/json", json_app)

    @app.on_event("startup")
    async def startup() -> None:
        app.state.testing = testing
        app.state.store = store

    @app.get("/get/")
    async def get(args: GetRequest) -> Response:
        cur = app.state.store.cursor()
        res = cur.get(**args.kwargs)

        # Check if the result is a pandas df
        if isinstance(res, dict):
            res = pd.DataFrame(res, index=[0])

        elif not isinstance(res, pd.DataFrame):
            res = pd.DataFrame(res)

        return df_to_json_response(res)

    @app.get("/mget/")
    async def mget(args: MgetRequest) -> Response:
        cur = app.state.store.cursor()
        res = cur.mget(
            **args.kwargs,
        )
        # Check if the result is a pandas df
        if isinstance(res, dict):
            res = pd.DataFrame(res, index=[0])

        elif not isinstance(res, pd.DataFrame):
            res = pd.DataFrame(res)

        return df_to_json_response(res)

    @app.post("/set_python/")
    async def set_python(
        args: Json[PartialSetRequest], file: UploadFile = File(...)
    ) -> typing.Any:
        args = args.dict()  # type: ignore
        content = await file.read()
        with BytesIO(content) as f:
            if file.content_type == "application/octet-stream":
                df = pd.read_parquet(f, engine="pyarrow")
                args["key_values"] = df.to_dict(orient="records")[0]  # type: ignore

        cur = app.state.store.cursor()
        return cur.set(
            relation=args["relation"],  # type: ignore
            identifier=args["identifier"],  # type: ignore
            key_values=args["key_values"],  # type: ignore
        )

    @app.get("/sql/")
    async def sql(args: SqlRequest) -> Response:
        cur = app.state.store.cursor()
        res = cur.sql(args.query, args.as_df)
        if isinstance(res, pd.DataFrame):
            return df_to_json_response(res)
        else:
            return Response(res, media_type="application/json")

    @app.post("/duplicate/")
    async def duplicate(data: DuplicateRequest) -> typing.Any:
        cur = app.state.store.cursor()
        return cur.duplicate(relation=data.relation, identifier=data.identifier)

    @app.post("/wait_for_trigger/")
    async def wait_for_trigger(request: Request) -> str:
        data = await request.body()
        trigger = parse_qs(data.decode("unicode_escape"))["trigger"][0]

        app.state.store.waitForTrigger(trigger)
        return trigger

    @app.get("/ping/")
    async def root() -> dict:
        return {"message": "Hello World"}

    @app.get("/session_id/")
    async def session_id() -> typing.Any:
        return app.state.store.session_id

    @app.on_event("shutdown")
    async def shutdown() -> None:
        if not app.state.testing:
            app.state.store.stop()

    # JSON API

    @json_app.post("/set/")
    async def json_set(args: SetRequest) -> Response:
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
    async def json_get(args: GetRequest) -> typing.Any:
        cur = app.state.store.cursor()

        try:
            res = cur.get(**args.kwargs)
            return res
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @json_app.get("/mget/")
    async def json_mget(args: MgetRequest) -> typing.Any:
        cur = app.state.store.cursor()
        try:
            res = cur.mget(**args.kwargs)
            return res
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @json_app.get("/sql/")
    async def json_sql(args: SqlRequest) -> typing.Any:
        cur = app.state.store.cursor()
        try:
            res = cur.sql(args.query, as_df=False)
            return res
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @json_app.get("/session_id/")
    async def json_session_id() -> typing.Any:
        return app.state.store.session_id

    return app
