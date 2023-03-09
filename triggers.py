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


class SuggestIdea(motion.Trigger):
    def setUp(self, cursor):
        # Set up the query suggestion model
        return {"cohere": cohere.Client(os.environ["COHERE_API_KEY"])}

    def shouldInfer(self, cursor, id, triggered_by):
        return True

    def infer(self, cursor, id, triggered_by):
        # Generate the query suggestions
        query = triggered_by.value
        prompt = (
            f"List 5 detailed outfit ideas for a woman to wear to {query}."
        )
        response = self.state["cohere"].generate(
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
            new_id = cursor.duplicate("query", id=id)
            cursor.set("query", id=new_id, key_values={"text_suggestion": s})

    def shouldFit(self, cursor, id, triggered_by):
        # Check if fit should be called
        return False

    def fit(self, cursor, id, triggered_by):
        # Fine-tune or fit the query suggestion model
        pass


class Retrieval(motion.Trigger):
    def setUp(self, cursor):
        # Set up the embedding model
        device = "cuda" if torch.cuda.is_available() else "cpu"
        model, preprocess = clip.load("ViT-B/32", device=device)

        # Set up state
        state = {
            "device": device,
            "model": model,
            "preprocess": preprocess,
            "k": 5,
        }
        index = self.createIndex(cursor)
        if index:
            state.update(index)
        return state

    def inferText(self, cursor, id, text):
        with torch.no_grad():
            text_inputs = clip.tokenize([text]).to(self.state["device"])
            text_features = self.state["model"].encode_text(text_inputs)

        # Search the FAISS index for the most similar image
        text_features = text_features.numpy()
        scores, indices = self.state["index"].search(
            text_features / np.linalg.norm(text_features, axis=1),
            self.state["k"],
        )
        for score, index in zip(scores[0], indices[0]):
            img_id = self.state["index_to_id"][index]
            new_id = cursor.duplicate("query", id=id)
            cursor.set(
                "query",
                id=new_id,
                key_values={"img_id": img_id, "img_score": score},
            )

    def shouldInfer(self, cursor, id, triggered_by):
        # Call infer only on text suggestions
        if triggered_by.key == "text_suggestion":
            return True

        return False

    def infer(self, cursor, id, triggered_by):
        self.inferText(cursor, id, triggered_by.value)

    def shouldFit(self, cursor, id, triggered_by):
        # Check if fit should be called
        if triggered_by.key == "feedback" and triggered_by.value == True:
            return True

        elif (
            triggered_by.namespace == "catalog"
            and triggered_by.key == "img_blob"
        ):
            return True

        else:
            return False

    def fit(self, cursor, id, triggered_by):
        if triggered_by.key == "img_blob":
            self.embedImage(
                cursor, id, triggered_by.value, self.state["model"]
            )

            # Add the normalized image to the FAISS index every 10 iterations
            if id % 10 == 0:
                index = self.createIndex(cursor)
                return index

            else:
                return {}

        # Fine-tune model every 5 positive feedbacks on the most recent
        # 100 positive feedbacks
        if triggered_by.key == "feedback":
            positive_feedback_ids = cursor.getIdsForKey(
                "query", "feedback", True
            )[-5:]

            if (
                len(positive_feedback_ids) > 0
                and len(positive_feedback_ids) % 5 == 0
            ):
                new_state = self.fineTune(cursor, positive_feedback_ids)
                return new_state
            else:
                return {}

    def fineTune(self, cursor, positive_feedback_ids):
        logging.info(
            f"Fine-tuning CLIP model on {len(positive_feedback_ids)} ids."
        )

        # Get image blobs and text suggestions
        text_suggestions_and_img_ids = cursor.mget(
            "query",
            ids=positive_feedback_ids,
            keys=["text_suggestion", "img_id"],
        )
        img_ids_and_blobs = cursor.mget(
            "catalog",
            ids=text_suggestions_and_img_ids.img_id.values,
            keys=["img_blob"],
        )
        img_blobs_and_captions = text_suggestions_and_img_ids.merge(
            img_ids_and_blobs, left_on="img_id", right_on="id"
        )[["img_blob", "text_suggestion"]].dropna()

        # Fine-tune model and update all the embeddings
        new_model = fine_tune_model(
            self.state["model"],
            img_blobs=img_blobs_and_captions.img_blob.values,
            captions=img_blobs_and_captions.text_suggestion.values,
        )

        ids_and_blobs = cursor.sql(
            "SELECT id, img_blob FROM fashion.catalog WHERE img_embedding IS NOT NULL"
        )

        # Re-embed all the images
        for _, row in ids_and_blobs.iterrows():
            self.embedImage(cursor, row["id"], row["img_blob"], new_model)

        new_state = {"model": new_model}
        new_state.update(self.createIndex(cursor))

        return new_state

    def embedImage(self, cursor, id, img_blob, model):
        with torch.no_grad():
            image_input = (
                self.state["preprocess"](Image.open(BytesIO(img_blob)))
                .unsqueeze(0)
                .to(self.state["device"])
            )
            image_features = model.encode_image(image_input)

        cursor.set(
            "catalog",
            id=id,
            key_values={"img_embedding": image_features.squeeze().tolist()},
        )

    def createIndex(self, cursor):
        index = faiss.IndexFlatIP(512)
        id_embedding = cursor.sql(
            "SELECT id, img_embedding FROM fashion.catalog WHERE img_embedding IS NOT NULL"
        )
        if len(id_embedding) == 0:
            return
        embeddings = np.stack(id_embedding["img_embedding"].values)
        ids = id_embedding["id"].values
        index.add(embeddings)
        new_index_to_id = {}
        for old_id in ids:
            new_index_to_id[len(new_index_to_id)] = old_id
        return {"index": index, "index_to_id": new_index_to_id}
