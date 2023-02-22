import numpy as np
import motion

from dataclasses import dataclass
from enum import Enum

# Step 1: Define the store schemas and create the store


class Retailer(Enum):
    NORDSTROM = 0
    REVOLVE = 1
    BLOOMINGDALES = 2


class QuerySource(Enum):
    OFFLINE = 0
    ONLINE = 1


@dataclass
class QuerySchema:
    id: int
    ts: int
    src: QuerySource
    query: str
    text_suggestion: str
    text_suggestion_page: int
    img_id: int
    img_score: int
    img_id_page: int


@dataclass
class CatalogSchema:
    id: int
    ts: int
    retailer: Retailer
    img_url: str
    img_desc: str
    img_html: str
    img_summary: str
    img_embedding: np.ndarray


store = motion.create_store(
    "fashion_search",
    schemas={"query": QuerySchema, "catalog": CatalogSchema},
)

# Step 2: Define the pipeline components. One amasses the catalog; the other generates the query suggestions. We first start with the catalog subpipeline.


def scrape_nordstrom(store):
    # Scrape the catalog and add the images to the store
    new_id = store.new_id("catalog")
    store.add(
        "catalog", {"id": new_id, "retailer": Retailer.NORDSTROM}
    )  # Add more fields as necessary
    # TODO: implement rest


class ImageEmbeddings(motion.Transform):
    def setUp(self):
        # Set up the image embedding model
        pass

    def shouldFit(self, store, new_id):
        # Check if fit should be called
        pass

    def fit(self, store, id):
        # Fine-tune or fit the image embedding model
        pass

    def transform(self, store, id):
        # Embed the image
        pass


# Then we write the query suggestion subpipeline


class QuerySuggestion(motion.Transform):
    def setUp(self):
        # Set up the query suggestion model
        pass

    def shouldFit(self, store, new_id):
        # Check if fit should be called
        pass

    def fit(self, store, id):
        # Fine-tune or fit the query suggestion model
        pass

    def transform(self, store, id):
        # Generate the query suggestions
        pass


# TODO(shreyashankar): pass in the schema/table that triggered an operation. Since it'll be triggered by both queries and retail items. Also should this class even exist?
class RetrieveRecommendation(motion.Transform):
    def setUp(self):
        # Set up the vector store to hold image embeddings
        pass

    def shouldFit(self, store, new_id):
        # Check if fit should be called
        pass

    def fit(self, store, id):
        # Amass the vector store
        pass

    def transform(self, store, id):
        # Retrieve the best images
        pass


# Step 3: Add the pipeline components as triggers. Triggers can be added as cron jobs or on the addition/change of a row in a table.
