import logging
import numpy as np
import motion

from dataclasses import dataclass
from rich import print

# create logger
logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)

# Step 1: Define the store schemas and create the store


class Retailer(motion.MEnum):
    NORDSTROM = "Nordstrom"
    REVOLVE = "Revolve"
    BLOOMINGDALES = "Bloomingdales"


class QuerySource(motion.MEnum):
    OFFLINE = "Offline"
    ONLINE = "Online"


class QuerySchema(motion.Schema):
    src: QuerySource
    query: str
    text_suggestion: str
    text_suggestion_page: int
    img_id: int
    img_score: int
    img_id_page: int


class CatalogSchema(motion.Schema):
    retailer: Retailer
    img_url: str
    img_desc: str
    img_html: str
    img_summary: str
    img_embedding: np.ndarray


store = motion.get_or_create_store(
    "fashion",
)
store.addNamespace("query", QuerySchema)
store.addNamespace("catalog", CatalogSchema)

# Step 2: Define the pipeline components. One amasses the catalog; the other generates the query suggestions. We first start with the catalog subpipeline.


def scrape_nordstrom(store):
    # Scrape the catalog and add the images to the store
    new_id = store.new_id("catalog")
    store.add(
        "catalog", {"id": new_id, "retailer": Retailer.NORDSTROM}
    )  # Add more fields as necessary
    # TODO: implement rest


class EmbedImage(motion.Transform):
    def setUp(self, store):
        # Set up the image embedding model
        pass

    def shouldFit(self, new_id, triggered_by):
        # Check if fit should be called
        pass

    def fit(self, id):
        # Fine-tune or fit the image embedding model
        pass

    def transform(self, id, triggered_by):
        # Embed the image
        pass


# Then we write the query suggestion subpipeline


class SuggestQuery(motion.Transform):
    def setUp(self, store):
        # Set up the query suggestion model
        pass

    def shouldFit(self, new_id, triggered_by):
        # Check if fit should be called
        pass

    def fit(self, id):
        # Fine-tune or fit the query suggestion model
        pass

    def transform(self, id, triggered_by):
        # Generate the query suggestions
        pass


# TODO(shreyashankar): pass in the schema/table that triggered an operation. Since it'll be triggered by both queries and retail items. Also should this class even exist?
class RetrieveRecommendation(motion.Transform):
    def setUp(self, store):
        # Set up the vector store to hold image embeddings
        pass

    def shouldFit(self, new_id, triggered_by):
        # Check if fit should be called
        pass

    def fit(self, id):
        # Amass the vector store
        pass

    def transform(self, id, triggered_by):
        # Retrieve the best images
        pass


# Step 3: Add the pipeline components as triggers. Triggers can be added as cron jobs or on the addition/change of a row in a table.

store.addTrigger(
    name="suggest_query", keys=["query.query"], trigger=SuggestQuery
)
store.addTrigger(
    name="embed_images", keys=["catalog.img_url"], trigger=EmbedImage
)
store.addTrigger(
    name="retrieve_recommendation",
    keys=["catalog.img_embedding", "query.text_suggestion"],
    trigger=RetrieveRecommendation,
)

# Step 4: Add the data to the store. This will trigger the pipeline components.

store.set(
    "catalog",
    primary_key={"id": store.getNewId("catalog")},
    key="img_url",
    value="https://...",
)

store.set(
    "query",
    primary_key={"id": store.getNewId("query")},
    key="query",
    value="hello",
)
store.set(
    "query",
    primary_key={"id": store.getNewId("query"), "text_suggestion_page": 1},
    key="query",
    value="hello",
)


print(store.con.execute("SELECT * FROM fashion.catalog").fetchdf())
print(store.con.execute("SELECT * FROM fashion.query").fetchdf())
