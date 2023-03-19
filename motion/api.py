from fastapi import FastAPI
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError

import logging
import pandas as pd

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


def create_app(store):
    app = FastAPI()

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ):
        detail = exc.errors()[0]["msg"]
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"detail": detail},
        )

    @app.on_event("startup")
    async def startup():
        app.state.store = store

    @app.get("/get/")
    async def get(args: MotionGet):
        cur = app.state.store.cursor()
        res = cur.get(**args.kwargs)
        if isinstance(res, pd.DataFrame):
            return res.to_dict("records")
        else:
            return res

    @app.get("/mget/")
    async def mget(args: MotionMget):
        cur = app.state.store.cursor()
        res = cur.mget(
            **args.kwargs,
        )
        if isinstance(res, pd.DataFrame):
            return res.to_dict("records")
        else:
            return res

    @app.post("/set/")
    async def set(args: MotionSet):
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
            return res.to_dict("records")
        else:
            return res

    @app.get("/ping/")
    async def root():
        return {"message": "Hello World"}

    @app.on_event("shutdown")
    async def shutdown():
        app.state.store.stop()

    return app
