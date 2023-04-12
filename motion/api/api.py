import os
import typing
from io import BytesIO
from urllib.parse import parse_qs

import pandas as pd
from fastapi import Depends, FastAPI, File, HTTPException, Request, Response, UploadFile
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import Json

from motion.api.models import *
from motion.store import Store


def df_to_json_response(df: pd.DataFrame) -> Response:
    return Response(
        df.to_parquet(engine="pyarrow", index=False),
        media_type="application/octet-stream",
    )


def create_fastapi_app(store: Store, testing: bool = False) -> FastAPI:
    # Security
    MOTION_API_TOKEN = os.environ.get("MOTION_API_TOKEN")
    if not MOTION_API_TOKEN:
        raise ValueError(
            "MOTION_API_TOKEN environment variable is not set. Please run `motion token` to generate a token, and export MOTION_API_TOKEN={result}."
        )

    scheme = HTTPBearer()

    def check_auth(
        credentials: HTTPAuthorizationCredentials = Depends(scheme),
    ) -> bool:
        if credentials.credentials != MOTION_API_TOKEN:
            raise HTTPException(
                status_code=401, detail="Invalid authentication credentials"
            )

        return True

    app = FastAPI(dependencies=[Depends(check_auth)])

    json_app = FastAPI(
        title="Motion JSON API",
        description="An API for calling basic Motion functions on JSON data.",
        dependencies=[Depends(check_auth)]
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
        res = cur.get(**args.__dict__)

        # Check if the result is a pandas df
        if isinstance(res, dict):
            res = pd.DataFrame([res])

        elif not isinstance(res, pd.DataFrame):
            res = pd.DataFrame(res)

        return df_to_json_response(res)

    @app.get("/mget/")
    async def mget(args: MgetRequest) -> Response:
        cur = app.state.store.cursor()
        res = cur.mget(
            **args.__dict__,
        )
        # Check if the result is a pandas df
        if isinstance(res, dict):
            res = pd.DataFrame([res])

        elif not isinstance(res, pd.DataFrame):
            res = pd.DataFrame(res)

        return df_to_json_response(res)

    @app.post("/set_python/")
    async def set_python(
        args: Json[PartialSetRequest], key_values: UploadFile = File(...)
    ) -> typing.Any:
        args = args.dict()  # type: ignore
        content = await key_values.read()
        with BytesIO(content) as f:
            if key_values.content_type == "application/octet-stream":
                df = pd.read_parquet(f, engine="pyarrow")
                args["key_values"] = df.to_dict(orient="records")[0]  # type: ignore
            else:
                raise HTTPException(status_code=400, detail="Invalid file type")

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

    @app.post("/checkpoint/")
    async def checkpoint() -> typing.Any:
        return app.state.store.checkpoint_pa()

    @app.on_event("shutdown")
    async def shutdown() -> None:
        if not app.state.testing:
            app.state.store.stop()

    # JSON API

    @json_app.post("/set/")
    async def json_set(args: SetRequest) -> typing.Any:
        cur = app.state.store.cursor()
        try:
            identifier = cur.set(
                relation=args.relation,
                identifier=args.identifier,
                key_values=args.key_values,
            )
            return identifier
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @json_app.get("/get/")
    async def json_get(args: GetRequest) -> typing.Any:
        cur = app.state.store.cursor()

        try:
            args = args.dict()  # type: ignore
            args["as_df"] = False  # type: ignore
            res = cur.get(**args)
            return res
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @json_app.get("/mget/")
    async def json_mget(args: MgetRequest) -> typing.Any:
        cur = app.state.store.cursor()
        try:
            args = args.dict()  # type: ignore
            args["as_df"] = False  # type: ignore
            res = cur.mget(**args)
            return res
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @json_app.get("/sql/")
    async def json_sql(args: SqlRequest) -> typing.Any:
        cur = app.state.store.cursor()
        try:
            res = cur.sql(args.query, as_df=True)
            return res.to_dict(orient="records")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @json_app.get("/session_id/")
    async def json_session_id() -> typing.Any:
        return app.state.store.session_id

    @json_app.post("/wait_for_trigger/")
    async def json_wait_for_trigger(args: WaitRequest) -> typing.Any:
        cur = app.state.store.cursor()
        try:
            app.state.store.waitForTrigger(args.trigger)
            return args.trigger
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    return app
