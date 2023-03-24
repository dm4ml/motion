import clip
import faiss
import numpy as np
import motion
import torch

from fashion.triggers.clipft import fine_tune_model
from io import BytesIO
from PIL import Image


class Retrieval(motion.Trigger):
    def routes(self):
        return [
            motion.Route(
                relation="query",
                key="text_suggestion",
                infer=self.suggestionToImage,
                fit=None,
            ),
            motion.Route(
                relation="closet",
                key="sd_img_blob",
                infer=self.closetToImage,
                fit=None,
            ),
            motion.Route(
                relation="catalog",
                key="img_blob",
                infer=None,
                fit=self.catalogToIndex,
            ),
            motion.Route(
                relation="query",
                key="feedback",
                infer=None,
                fit=self.fineTune,
            ),
        ]

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
            "streaming_image_count": 0,  # To keep track of how many catalog images we've seen. We add to the index every 10 new images.
        }
        index = self._createIndex(cursor)
        if index:
            state.update(index)

        return state

    def suggestionToImage(self, cursor, triggered_by):
        text = triggered_by.value
        with torch.no_grad():
            text_inputs = clip.tokenize([text]).to(self.state["device"])
            text_features = self.state["model"].encode_text(text_inputs)

        # Search the FAISS index for the most similar image
        scores, img_ids = self._searchIndex(text_features)
        self._writeSimilarImages(cursor, triggered_by, scores, img_ids)

    def closetToImage(self, cursor, triggered_by):
        # Run CLIP on the uploaded image to get the image features,
        # then find similar images in the catalog
        image_features = self._embedImage(
            triggered_by.value, self.state["model"]
        )
        # Search the FAISS index for the most similar image
        scores, img_ids = self._searchIndex(image_features)
        self._writeSimilarImages(cursor, triggered_by, scores, img_ids)

    def catalogToIndex(self, cursor, triggered_by) -> dict:
        image_features = self._embedImage(
            triggered_by.value, self.state["model"]
        )
        cursor.set(
            triggered_by.relation,
            identifier=triggered_by.identifier,
            key_values={"img_embedding": image_features.squeeze().tolist()},
        )

        # Add the normalized image to the FAISS index every 10 iterations
        if self.state["streaming_image_count"] % 10 == 0:
            new_state = self._createIndex(cursor)

        else:
            new_state = {}

        new_state.update(
            {"streaming_image_count": self.state["streaming_image_count"] + 1}
        )
        return new_state

    def fineTune(self, cursor, triggered_by):
        positive_feedback_ids = cursor.getIdsForKey("query", "feedback", True)

        # Only fine-tune if we have seen at least 5 positive feedbacks
        if (
            len(positive_feedback_ids) % 5 != 0
            or len(positive_feedback_ids) == 0
        ):
            return {}

        print(f"Fine-tuning CLIP model on {len(positive_feedback_ids)} ids.")

        # Get image blobs and text suggestions
        text_suggestions_and_img_ids = cursor.mget(
            "query",
            identifiers=positive_feedback_ids,
            keys=["text_suggestion", "catalog_img_id"],
            as_df=True,
        )
        img_ids_and_blobs = cursor.mget(
            "catalog",
            identifiers=text_suggestions_and_img_ids.catalog_img_id.values,
            keys=["img_blob"],
            as_df=True,
        )
        img_blobs_and_captions = text_suggestions_and_img_ids.merge(
            img_ids_and_blobs, left_on="catalog_img_id", right_on="identifier"
        )[["img_blob", "text_suggestion"]].dropna()

        # Fine-tune model and update all the embeddings
        new_model = fine_tune_model(
            self.state["model"],
            img_blobs=img_blobs_and_captions.img_blob.values,
            captions=img_blobs_and_captions.text_suggestion.values,
        )

        ids_and_blobs = cursor.sql(
            "SELECT identifier, img_blob FROM catalog WHERE img_embedding IS NOT NULL"
        )

        # Re-embed all the images
        for _, row in ids_and_blobs.iterrows():
            image_features = self._embedImage(row["img_blob"], new_model)
            cursor.set(
                "catalog",  # This isn't the triggered_by relation!
                identifier=row["identifier"],
                key_values={
                    "img_embedding": image_features.squeeze().tolist()
                },
            )

        new_state = {"model": new_model}
        new_state.update(self._createIndex(cursor))

        return new_state

    def _searchIndex(self, features):
        features = features.numpy()
        scores, indices = self.state["index"].search(
            features / np.linalg.norm(features, axis=1), self.state["k"]
        )
        img_ids = [self.state["index_to_id"][index] for index in indices[0]]
        return scores[0], img_ids

    def _writeSimilarImages(self, cursor, triggered_by, scores, img_ids):
        for score, img_id in zip(scores, img_ids):
            new_id = cursor.duplicate(
                triggered_by.relation, identifier=triggered_by.identifier
            )
            cursor.set(
                triggered_by.relation,
                identifier=new_id,
                key_values={
                    "catalog_img_id": img_id,
                    "catalog_img_score": score,
                },
            )

    def _embedImage(self, img_blob, model):
        with torch.no_grad():
            image_input = (
                self.state["preprocess"](Image.open(BytesIO(img_blob)))
                .unsqueeze(0)
                .to(self.state["device"])
            )
            image_features = model.encode_image(image_input)

        return image_features

    def _createIndex(self, cursor):
        index = faiss.IndexFlatIP(512)
        id_embedding = cursor.sql(
            "SELECT identifier, img_embedding FROM catalog WHERE img_embedding IS NOT NULL"
        )
        if len(id_embedding) == 0:
            return
        embeddings = np.stack(id_embedding["img_embedding"].values)
        ids = id_embedding["identifier"].values
        index.add(embeddings)
        new_index_to_id = {}
        for old_id in ids:
            new_index_to_id[len(new_index_to_id)] = old_id
        return {"index": index, "index_to_id": new_index_to_id}
