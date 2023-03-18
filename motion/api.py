from fastapi import FastAPI


def create_app(store):
    app = FastAPI()

    @app.on_event("startup")
    async def startup():
        app.state.store = store

    @app.get("/get/")
    async def get(args: dict):
        namespace = args["namespace"]
        identifier = args["identifier"]
        keys = args["keys"]
        kwargs = args["kwargs"] or {}
        cur = app.state.store.cursor()
        return cur.get(namespace, identifier, keys, **kwargs)

    @app.get("/mget/")
    async def mget(args: dict):
        namespace = args["namespace"]
        identifiers = args["identifiers"]
        keys = args["keys"]
        kwargs = args["kwargs"] or {}
        cur = app.state.store.cursor()
        return cur.mget(namespace, identifiers, keys, **kwargs)

    @app.post("/set/")
    async def set(args: dict):
        namespace = args["namespace"]
        identifier = args["identifier"]
        key_values = args["key_values"]
        run_duplicate_triggers = args["run_duplicate_triggers"] or False
        cur = app.state.store.cursor()
        return cur.set(
            namespace, identifier, key_values, run_duplicate_triggers
        )

    @app.get("/get_new_id/")
    async def get_new_id(kwargs: dict):
        cur = app.state.store.cursor()
        return cur.getNewId(**kwargs)

    @app.get("/sql/")
    async def sql(query: str):
        cur = app.state.store.cursor()
        return cur.sql(query)

    @app.on_event("shutdown")
    async def shutdown():
        app.state.store.stop()

    return app
