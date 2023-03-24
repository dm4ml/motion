from fashion.schemas import (
    QuerySchema,
    CatalogSchema,
    ClosetSchema,
)
from fashion.triggers import (
    Retrieval,
    SuggestIdea,
    ExtractOutfit,
    scrape_everlane,
)

MCONFIG = {
    "application": {
        "name": "fashion",
        "author": "shreyashankar",
        "version": "0.1",
    },
    "relations": {
        "query": QuerySchema,
        "catalog": CatalogSchema,
        "closet": ClosetSchema,
    },
    "triggers": {
        SuggestIdea: ["query.query"],
        ExtractOutfit: ["closet.img_blob"],
        Retrieval: [
            "catalog.img_blob",
            "query.text_suggestion",
            "query.feedback",
            "closet.sd_img_blob",
        ],
        scrape_everlane: ["0 * * * *"],
    },
    "trigger_params": {ExtractOutfit: {"strength": 0.5}},
    "checkpoint": "0 * * * *",
}
