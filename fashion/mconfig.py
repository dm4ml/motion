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
    local_image_path_to_blob,
)

MCONFIG = {
    "application": {
        "name": "fashion",
        "author": "shreyashankar",
        "version": "0.1",
    },
    "namespaces": {
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
        local_image_path_to_blob: ["closet.img_path"],
    },
    "checkpoint": "0 * * * *",
}
