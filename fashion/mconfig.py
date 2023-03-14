from fashion.schemas import QuerySchema, CatalogSchema
from fashion.triggers import Retrieval, SuggestIdea, scrape_everlane_sale

mconfig = {
    "application": {
        "name": "chatbot",
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
        scrape_everlane_sale: ["*/1 * * * *"],
    },
}

if __name__ == "__main__":
    import motion

    # TODO(fix this error where store needs to start before trigger objects are created)
    store = motion.init(mconfig)

    print("Initialized store")
