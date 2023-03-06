import clip
import cohere
import faiss
import logging
import json
import numpy as np
import motion
import os
import pandas as pd
import re
import requests
import torch

from io import BytesIO
from PIL import Image
from typing import TypeVar

from bs4 import BeautifulSoup


# Step 1: Define the store schemas


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
    query_id: int
    query: str
    text_suggestion: str
    img_id: int
    img_score: float


class CatalogSchema(motion.Schema):
    retailer: Retailer
    img_url: str
    img_name: str
    permalink: str
    img_embedding: TypeVar("FLOAT[]")


# Step 2: Define the scrapers (just Everlane for now)


def scrape_everlane_sale(store):
    # Scrape the catalog and add the images to the store
    urls = [
        # "https://www.everlane.com/collections/womens-sale-2",
        "https://www.everlane.com/collections/womens-all-tops",
        "https://www.everlane.com/collections/womens-tees",
        "https://www.everlane.com/collections/womens-sweaters",
        "https://www.everlane.com/collections/womens-sweatshirts",
        "https://www.everlane.com/collections/womens-bodysuits",
        "https://www.everlane.com/collections/womens-jeans",
        "https://www.everlane.com/collections/womens-bottoms",
        "https://www.everlane.com/collections/womens-skirts-shorts",
        "https://www.everlane.com/collections/womens-dresses",
        "https://www.everlane.com/collections/womens-outerwear",
        "https://www.everlane.com/collections/womens-underwear",
        "https://www.everlane.com/collections/womens-perform",
        "https://www.everlane.com/collections/swimwear",
        "https://www.everlane.com/collections/womens-shoes",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"
    }
    product_info = []
    for url in urls:
        r = requests.get(url=url, headers=headers)

        soup = BeautifulSoup(r.content, "html5lib")

        res = soup.find("script", attrs={"id": "__NEXT_DATA__"})
        products = json.loads(res.contents[0])["props"]["pageProps"][
            "fallbackData"
        ]["products"]

        for product in products:
            img_url = product["albums"]["square"][0]["src"]
            img_name = product["displayName"]
            permalink = product["permalink"]
            product_info.append(
                {
                    "img_url": img_url,
                    "img_name": img_name,
                    "permalink": permalink,
                }
            )

    # Delete duplicates
    df = pd.DataFrame(product_info)
    df = (
        df.drop_duplicates(subset=["img_url"])
        .sample(frac=1)
        .reset_index(drop=True)
    )
    logging.info(f"Found {len(df)} unique products")

    for _, product_row in df.head(20).iterrows():
        new_id = store.getNewId("catalog")
        product = product_row.to_dict()
        product.update({"retailer": Retailer.EVERLANE})
        store.set("catalog", id=new_id, key_values=product)


# Step 3: Define the pipeline components. One amasses the catalog; the other generates the query suggestions. We first start with the catalog subpipeline.


class SuggestIdea(motion.Transform):
    def setUp(self, store):
        # Set up the query suggestion model
        self.store = store
        self.co = cohere.Client(os.environ["COHERE_API_KEY"])

    def shouldFit(self, new_id, triggered_by):
        # Check if fit should be called
        return False

    def fit(self, id, triggered_by):
        # Fine-tune or fit the query suggestion model
        pass

    def transform(self, id, triggered_by):
        # Generate the query suggestions
        query = triggered_by.value
        prompt = (
            f"List 5 detailed outfit ideas for a woman to wear to {query}."
        )
        response = self.co.generate(
            prompt=prompt,
            model="command-xlarge-nightly",
            max_tokens=300,
            temperature=0.9,
            k=0,
            p=0.75,
            stop_sequences=[],
            return_likelihoods="NONE",
        )
        text = response[0].text
        suggestions = [s.strip() for s in text.split("\n")[:5]]
        suggestions = [re.sub("[1-9]. ", "", s) for s in suggestions]

        for s in suggestions:
            new_id = self.store.duplicate("query", id=id)
            self.store.set(
                "query", id=new_id, key_values={"text_suggestion": s}
            )


class Retrieval(motion.Transform):
    def setUp(self, store):
        # Set up the embedding model
        self.store = store
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model, self.preprocess = clip.load("ViT-B/32", device=self.device)

        # Set up the FAISS index
        self.index = faiss.IndexFlatIP(512)
        self.index_to_id = {}
        self.k = 10

    def shouldFit(self, new_id, triggered_by):
        # Check if fit should be called
        return False

    def fit(self, id, triggered_by):
        # Fine-tune or fit the image embedding model
        pass

    def transformImage(self, id, image_url):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"
        }
        response = requests.get(image_url, headers=headers)

        with torch.no_grad():
            image_input = (
                self.preprocess(Image.open(BytesIO(response.content)))
                .unsqueeze(0)
                .to(self.device)
            )
            image_features = self.model.encode_image(image_input)

        self.store.set(
            "catalog",
            id=id,
            key_values={"img_embedding": image_features.squeeze().tolist()},
        )

        # Add the normalized image to the FAISS index
        image_features = image_features.numpy()
        self.index.add(image_features / np.linalg.norm(image_features, axis=1))
        self.index_to_id[len(self.index_to_id)] = id

    def transformText(self, id, text):
        with torch.no_grad():
            text_inputs = clip.tokenize([text]).to(self.device)
            text_features = self.model.encode_text(text_inputs)

        # Search the FAISS index for the most similar image
        text_features = text_features.numpy()
        scores, indices = self.index.search(
            text_features / np.linalg.norm(text_features, axis=1), self.k
        )
        for score, index in zip(scores[0], indices[0]):
            img_id = self.index_to_id[index]
            new_id = self.store.duplicate("query", id=id)
            self.store.set(
                "query",
                id=new_id,
                key_values={"img_id": img_id, "img_score": score},
            )

    def transform(self, id, triggered_by):
        # Embed the image
        if triggered_by.key == "img_url":
            self.transformImage(id, triggered_by.value)

        # If the trigger key is the text suggestion, then we need to retrieve the image
        elif triggered_by.key == "text_suggestion":
            self.transformText(id, triggered_by.value)
