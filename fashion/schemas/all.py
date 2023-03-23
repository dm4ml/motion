import motion
from typing import List


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
    catalog_img_id: str
    catalog_img_score: float
    feedback: bool


class CatalogSchema(motion.Schema):
    retailer: Retailer
    img_url: str
    img_blob: bytes
    img_name: str
    permalink: str
    img_embedding: List[float]


class ClosetSchema(motion.Schema):
    username: str
    img_blob: bytes
    sd_img_blob: bytes
    catalog_img_id: str
    catalog_img_score: str
