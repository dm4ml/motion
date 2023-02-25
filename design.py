import clip
import cohere
import logging
import numpy as np
import motion
import os
import re
import requests
import torch

from io import BytesIO
from PIL import Image
from typing import TypeVar

from dataclasses import dataclass
from langchain.llms import Cohere
from langchain import PromptTemplate, LLMChain
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
    img_embedding: TypeVar("FLOAT[]")


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
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model, self.preprocess = clip.load("ViT-B/32", device=self.device)

    def shouldFit(self, new_id, triggered_by):
        # Check if fit should be called
        return False

    def fit(self, id, context):
        # Fine-tune or fit the image embedding model
        pass

    def transform(self, id, triggered_by):
        # Embed the image
        image_url = triggered_by.value
        # image_url = self.store.get("catalog", id=id, keys=["img_url"])[
        #     "img_url"
        # ]
        response = requests.get(image_url)
        with torch.no_grad():
            image_input = (
                self.preprocess(Image.open(BytesIO(response.content)))
                .unsqueeze(0)
                .to(self.device)
            )
            image_features = self.model.encode_image(image_input).squeeze()

        self.store.set(
            "catalog",
            id=id,
            key="img_embedding",
            value=image_features.tolist(),
        )


# Then we write the query suggestion subpipeline


class SuggestIdea(motion.Transform):
    def setUp(self, store):
        # Set up the query suggestion model
        self.store = store
        self.co = cohere.Client(os.environ["COHERE_API_KEY"])

    def shouldFit(self, new_id, triggered_by):
        # Check if fit should be called
        return False

    def fit(self, id, context):
        # Fine-tune or fit the query suggestion model
        pass

    def transform(self, id, triggered_by):
        # Generate the query suggestions
        query = triggered_by.value
        prompt = (
            f"List 5 detailed outfit ideas for a woman to wear to {query}:\n1."
        )
        response = self.co.generate(
            prompt=prompt,
            max_tokens=100,
            temperature=0.5,
            num_generations=1,
            stop_sequences=["--"],
            frequency_penalty=0.4,
        )
        text = response[0].text
        suggestions = [s.strip() for s in text.split("\n")[:5]]
        suggestions = [re.sub("[1-9]. ", "", s) for s in suggestions]
        for s in suggestions:
            new_id = store.duplicate("query", id=id)
            self.store.set("query", id=new_id, key="text_suggestion", value=s)


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
        print(
            "Transforming in RetrieveRecommendation! Triggered by: ",
            triggered_by,
        )


# Step 3: Add the pipeline components as triggers. Triggers can be added as cron jobs or on the addition/change of a row in a table.

store.addTrigger(
    name="suggest_idea", keys=["query.query"], trigger=SuggestIdea
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
    value="https://media.everlane.com/image/upload/c_fill,w_640,ar_1:1,q_auto,dpr_1.0,g_face:center,f_auto,fl_progressive:steep/i/6ca26f26_2313",
)

store.set(
    "query",
    id=None,
    key="query",
    value="a club in Vegas",
)
# store.set(
#     "query",
#     id=None,
#     key="query",
#     value="hello2",
# )

# new_id = store.duplicate("query", id=1)


print(store.con.execute("SELECT * FROM fashion.catalog").fetchdf())
print(store.con.execute("SELECT * FROM fashion.query").fetchdf())
