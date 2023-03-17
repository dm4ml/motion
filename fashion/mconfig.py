from fashion.schemas import QuerySchema, CatalogSchema
from fashion.triggers import Retrieval, SuggestIdea, scrape_everlane

MCONFIG = {
    "application": {
        "name": "fashion",
        "author": "shreyashankar",
        "version": "0.1",
    },
    "namespaces": {"query": QuerySchema, "catalog": CatalogSchema},
    "triggers": {
        SuggestIdea: ["query.query"],
        Retrieval: [
            "catalog.img_blob",
            "query.text_suggestion",
            "query.feedback",
        ],
        scrape_everlane: ["*/1 * * * *"],
    },
    "datastore_prefix": "datastores",
    "checkpoint": "0 * * * *",
}
