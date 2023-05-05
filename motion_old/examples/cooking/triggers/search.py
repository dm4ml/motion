import os
import typing
from collections import namedtuple
from typing import Dict, List, Tuple

import cohere
import faiss
import numpy as np

import motion

IngredientsList = namedtuple("IngredientsList", ["identifier", "ingredients"])


class SearchRecipe(motion.Trigger):
    def routes(self) -> List[motion.Route]:
        return [
            motion.Route(
                relation="Recipe",
                key="ingredients",
                infer=None,
                fit=self.addRecipeToIndex,
            ),
            motion.Route(
                relation="Query",
                key="ingredients",
                infer=self.findNearestRecipes,
                fit=None,
            ),
        ]

    def setUp(self, cursor: motion.Cursor) -> Dict:
        # Set up the recipe index and cohere client
        co = cohere.Client(os.environ["COHERE_API_KEY"])
        recipe_index = faiss.IndexFlatIP(4096)
        recipe_index_to_id: Dict[int, str] = {}

        ingredients_df = cursor.sql("SELECT identifier, ingredients FROM Recipe")

        if len(ingredients_df) > 0:
            ingredients_list = ingredients_df["ingredients"].tolist()
            recipe_ids = ingredients_df["identifier"].tolist()

            recipe_index, recipe_index_to_id = self._populateRecipeIndex(
                co,
                ingredients_list,
                recipe_ids,
                recipe_index,
                recipe_index_to_id,
            )

        return {
            "cohere": co,
            "recipe_stream": [],
            "recipe_index": recipe_index,
            "recipe_index_to_id": recipe_index_to_id,
        }

    def _populateRecipeIndex(
        self,
        co: cohere.Client,
        ingredients_list: List[str],
        recipe_ids: List[str],
        recipe_index: faiss.IndexFlatIP,
        recipe_index_to_id: Dict[int, str],
    ) -> Tuple[faiss.IndexFlatIP, Dict[int, str]]:
        response = co.embed(
            texts=ingredients_list,
        ).embeddings
        embedding_list = np.array(response)
        # Normalize the embeddings
        embedding_list = embedding_list / np.linalg.norm(
            embedding_list, axis=1, keepdims=True
        )

        # Add the embeddings to the index
        recipe_index.add(embedding_list)
        for rid in recipe_ids:
            recipe_index_to_id[len(recipe_index_to_id)] = rid

        return recipe_index, recipe_index_to_id

    def addRecipeToIndex(
        self,
        cursor: motion.Cursor,
        trigger_context: motion.TriggerElement,
        infer_context: typing.Any,
    ) -> Dict:
        # Keep a stream of the last 20 recipes,
        # adding them to the index every 20 iterations.
        # This is to speed up the process of adding recipes

        recipe_stream = self.state["recipe_stream"]
        recipe_stream.append(
            IngredientsList(trigger_context.identifier, trigger_context.value)
        )
        new_state = {"recipe_stream": recipe_stream}

        # Every 20 recipes, add them to the index
        if len(recipe_stream) % 20 == 0:
            # Get the embeddings
            ingredients_list = [r.ingredients for r in recipe_stream]
            recipe_ids = [r.identifier for r in recipe_stream]
            recipe_index, recipe_index_to_id = self._populateRecipeIndex(
                self.state["cohere"],
                ingredients_list,
                recipe_ids,
                self.state["recipe_index"],
                self.state["recipe_index_to_id"],
            )

            new_state.update(
                {
                    "recipe_stream": [],
                    "recipe_index": recipe_index,
                    "recipe_index_to_id": recipe_index_to_id,
                }
            )

        return new_state

    def _searchIndex(self, features: np.ndarray) -> Tuple[np.ndarray, List[str]]:
        scores, indices = self.state["recipe_index"].search(
            features / np.linalg.norm(features, axis=1),
            self.params["num_recipe_results"],
        )
        recipe_ids = [self.state["recipe_index_to_id"][index] for index in indices[0]]
        return scores[0], recipe_ids

    def findNearestRecipes(
        self, cursor: motion.Cursor, trigger_context: motion.TriggerElement
    ) -> None:
        # Find the nearest recipes for the query
        ingredients = trigger_context.value
        response = self.state["cohere"].embed(texts=[ingredients])
        embedding = np.array(response.embeddings[0]).reshape(1, -1)

        scores, recipe_ids = self._searchIndex(embedding)
        for score, recipe_id in zip(scores, recipe_ids):
            duplicate_id = cursor.duplicate(
                relation=trigger_context.relation,
                identifier=trigger_context.identifier,
            )
            cursor.set(
                relation=trigger_context.relation,
                identifier=duplicate_id,
                key_values={"recipe_id": recipe_id, "recipe_score": score},
            )
