import motion
from typing import TypeVar


class Retailer(motion.MEnum):
    NORDSTROM = "Nordstrom"
    REVOLVE = "Revolve"
    BLOOMINGDALES = "Bloomingdales"
    EVERLANE = "Everlane"


class QuerySource(motion.MEnum):
    OFFLINE = "Offline"
    ONLINE = "Online"


class QuerySchema(motion.Schema):
    src: QuerySource
    query_id: str
    query: str
    text_suggestion: str
    img_id: str
    img_score: float
    feedback: bool


class CatalogSchema(motion.Schema):
    retailer: Retailer
    img_url: str
    img_blob: TypeVar("BLOB")
    img_name: str
    permalink: str
    img_embedding: TypeVar("FLOAT[]")


class ClosetSchema(motion.Schema):
    username: str
    img_path: str
    img_blob: TypeVar("BLOB")
    img_embedding: TypeVar("FLOAT[]")
