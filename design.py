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
    query_id: int
    query: str
    text_suggestion: str
    img_id: int
    img_score: int


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
        self.store = store

    def shouldFit(self, new_id, triggered_by):
        # Check if fit should be called
        return False

    def fit(self, id, context):
        # Fine-tune or fit the image embedding model
        print("Fitting in EmbedImage!")
        pass

    def transform(self, id, triggered_by):
        # Embed the image
        print("Transforming in EmbedImage!")


# Then we write the query suggestion subpipeline


class SuggestQuery(motion.Transform):
    def setUp(self, store):
        # Set up the query suggestion model
        self.store = store

    def shouldFit(self, new_id, triggered_by):
        # Check if fit should be called
        return True

    def fit(self, id, context):
        # Fine-tune or fit the query suggestion model
        print("Fitting in SuggestQuery!")

    def transform(self, id, triggered_by):
        # Generate the query suggestions
        print("Transforming in SuggestQuery! ID: ", id)


class RetrieveRecommendation(motion.Transform):
    def setUp(self, store):
        # Set up the vector store to hold image embeddings
        self.store = store

    def shouldFit(self, new_id, triggered_by):
        # Check if fit should be called
        pass

    def fit(self, id, context):
        # Amass the vector store
        pass

    def transform(self, id, triggered_by):
        # Retrieve the best images
        print("Transforming in RetrieveRecommendation! ID: ", id)


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
    id=None,
    key="img_url",
    value="https://...",
)

store.set(
    "query",
    id=None,
    key="query",
    value="hello",
)
store.set(
    "query",
    id=None,
    key="query",
    value="hello2",
)

new_id = store.duplicate("query", id=1)


print(store.con.execute("SELECT * FROM fashion.catalog").fetchdf())
print(store.con.execute("SELECT * FROM fashion.query").fetchdf())
