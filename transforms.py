import clip
import cohere
import faiss
import logging
import numpy as np
import motion
import os
import re
import torch

from clipft import fine_tune_model
from io import BytesIO
from PIL import Image

# Define the pipeline transforms. One amasses the catalog; the other
# generates the query suggestions. We first start with the catalog subpipeline.


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

    def shouldInfer(self, id, triggered_by):
        return True

    def infer(self, id, triggered_by):
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
        suggestions = [s for s in suggestions if s != ""]

        for s in suggestions:
            new_id = self.store.duplicate("query", id=id)
            self.store.set(
                "query", id=new_id, key_values={"text_suggestion": s}
            )


# TODO(shreyashankar): should not be modifying state in transform methods
class Retrieval(motion.Transform):
    def setUp(self, store):
        # Set up the embedding model
        self.store = store
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model, self.preprocess = clip.load("ViT-B/32", device=self.device)

        # Set up the FAISS index
        self.index = faiss.IndexFlatIP(512)
        self.index_to_id = {}
        self.k = 5

    def shouldFit(self, new_id, triggered_by):
        # Check if fit should be called
        if (
            triggered_by.namespace == "query"
            and triggered_by.key == "feedback"
            and triggered_by.value == True
        ):
            return True

        else:
            return False

    def fit(self, id, triggered_by):
        # Fine-tune model every 5 positive feedbacks on the most recent
        # 100 positive feedbacks

        positive_feedback_ids = self.store.getIdsForKey(
            "query", "feedback", True
        )[:100]

        if (
            len(positive_feedback_ids) > 0
            and len(positive_feedback_ids) % 5 == 0
        ):
            logging.info(
                f"Fine-tuning CLIP model on {len(positive_feedback_ids)} ids."
            )

            # Get image blobs and text suggestions
            text_suggestions_and_img_ids = self.store.mget(
                "query",
                ids=positive_feedback_ids,
                keys=["text_suggestion", "img_id"],
            )
            img_ids_and_blobs = self.store.mget(
                "catalog",
                ids=text_suggestions_and_img_ids.img_id.values,
                keys=["img_blob"],
            )
            img_blobs_and_captions = text_suggestions_and_img_ids.merge(
                img_ids_and_blobs, left_on="img_id", right_on="id"
            )[["img_blob", "text_suggestion"]].dropna()

            # Fine-tune model
            new_model = fine_tune_model(
                self.model,
                img_blobs=img_blobs_and_captions.img_blob.values,
                captions=img_blobs_and_captions.text_suggestion.values,
            )
            self.model = new_model
            # TODO(shreyashankar): need to rerun model on all the previous images ??

    def inferImage(self, id, img_blob):
        with torch.no_grad():
            image_input = (
                self.preprocess(Image.open(BytesIO(img_blob)))
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

    def inferText(self, id, text):
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

    def shouldInfer(self, id, triggered_by):
        # Don't call transform if trigger element is feedback
        if (
            triggered_by.namespace == "query"
            and triggered_by.key == "feedback"
            and triggered_by.value == True
        ):
            return False

        else:
            return True

    def infer(self, id, triggered_by):
        # Embed the image
        if triggered_by.key == "img_blob":
            self.inferImage(id, triggered_by.value)

        # If the trigger key is the text suggestion, then we need to retrieve the image
        elif triggered_by.key == "text_suggestion":
            self.inferText(id, triggered_by.value)
