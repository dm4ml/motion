# (Starter) Recipe Retrieval Engine

In this tutorial, we'll build a recipe retrieval engine using Motion. This application will take a user's list of ingredients and return some recipes that they can make with those ingredients.

## Getting Started

To get started, we'll create a new Motion application. If you haven't already, install Motion with `pip install motion-python` and then run the `motion example` command:

```bash
$ motion example
Example application name: cooking
Your name: shreyashankar
```

If you navigate into the `cooking` directory (`cd cooking`), you'll see the following directory structure:

    cooking/
    ├── schemas/
    |   |── __init__.py
    │   └── all.py
    ├── triggers/
    |   |── __init__.py
    |   |── scrape.py
    │   └── search.py
    ├── dashboard.py
    ├── mconfig.py
    |── requirements.txt
    └── test.py

Create a virtual environment (e.g., via Conda) and install the dependencies:

```bash
$ pip install -r requirements.txt
```

## Relations

Our application has two relations: `Recipe` and `Query`. In Motion, you typically want to create a relation for each end-to-end pipeline in your application. In this case, we want a pipeline to continually scrape recipes from a website and another pipeline to search for recipes based on a user's ingredients.

```py title="schemas/all.py"
from enum import Enum

import motion


class RecipeSource(Enum):
    BONAPPETIT = "Bon Appetit"


class Recipe(motion.Schema):
    src: RecipeSource
    title: str
    description: str
    ingredients: str
    instructions: str
    image_url: str # URL to the image of the recipe
    recipe_url: str # URL to the recipe on the source website


class Query(motion.Schema):
    username: str
    ingredients: str
    recipe_id: str
    recipe_score: float
    feedback: bool
```

Note that we've also defined a `RecipeSource` enum. This is useful for us to keep track of where the recipe came from. In this application, we'll only be scraping recipes from Bon Appetit, but in a real application, you might want to scrape from multiple sources.

Then, in `schemas/__init__.py`, we'll define the relations to be used later in the application:

```py title="schemas/__init__.py"
from schemas.all import Recipe, RecipeSource, Query

__all__ = [
    "Recipe",
    "RecipeSource",
    "Query",
]
```

## Triggers

We have two triggers in this application: `scrape` and `search`. The `scrape` trigger will scrape recipes from Bon Appetit and store them in the `Recipe` relation. The `search` trigger will take a user's ingredients and return a list of recipes that they can make.

### Scrape

The `scrape` trigger will scrape recipes from Bon Appetit and store them in the `Recipe` relation. We'll use the `requests` library to make HTTP requests and the `BeautifulSoup` library to parse the HTML. We only show the trigger code here, but you can find the full code in `triggers/scrape.py` (e.g., parsing the ingredients and instructions is omitted for brevity).

```py title="triggers/scrape.py"
import asyncio
import json
from typing import Any, Dict, List

import aiohttp
import pandas as pd
import requests
from bs4 import BeautifulSoup
from schemas import RecipeSource

import motion


class ScrapeBonAppetit(motion.Trigger):
    def routes(self) -> List[motion.Route]:
        return [
            motion.Route(
                relation="",
                key="0 * * * *", # (1)!
                infer=self.scrape, # On trigger, run the scrape method
                fit=None, # We don't need to fit any models
            )
        ]

    def setUp(self, cursor: motion.Cursor) -> Dict:
        return {}

    def scrape(
        self, cursor: motion.Cursor, trigger_context: motion.TriggerElement # (2)!
    ) -> None:
        # Set lower time bound to avoid scraping recipes we've already scraped
        lower_bound = cursor.sql(
            "SELECT MAX(create_at) AS lower_bound FROM Recipe WHERE src='Bon Appetit'"
        )["lower_bound"].values[0]
        if pd.isnull(lower_bound):
            # If we haven't scraped any recipes yet, set lower bound to 2020
            lower_bound = "2019-01-01"
        else:
            lower_bound = str(lower_bound)

        url_list = asyncio.run(
            fetch_all_urls("https://www.bonappetit.com/sitemap.xml", lower_bound)
        ) # (3)!

        print(f"Found {len(url_list)} recipes.")

        recipes = asyncio.run(scrape_all_recipes(url_list))
        print(f"Scraped {len(recipes)} recipes.")

        # Add source to each recipe and insert into Recipe relation
        for recipe in recipes:
            recipe.update({"src": RecipeSource.BONAPPETIT})
            cursor.set(relation="Recipe", identifier="", key_values=recipe) # (4)!
```

1. The `key` argument is a cron expression that specifies when the trigger should run. In this case, we want the trigger to run every hour. You can read more about cron expressions [here](https://crontab.guru/).
2. For cron-scheduled trigggers, the `trigger_context` argument is the following: `TriggerElement(relation="_cron", identifier="SCHEDULED", key=self.cron_expression, value=None)`. There is no key-value pair that triggered the trigger, because the trigger runs on a schedule.
3. We use `asyncio` to make HTTP requests asynchronously. This is useful because we're making a lot of HTTP requests, and we don't want to block the main thread while we're waiting for the responses. `fetch_all_urls` and `scrape_all_recipes` are helper functions, also defined in `triggers/scrape.py`.
4. We use the `cursor` to insert the recipe into the `Recipe` relation. The `identifier` argument is an empty string because we don't have a unique identifier for each recipe. The `key_values` argument is a dictionary of the recipe's attributes.

!!! note
    The `set` method will potentially fire other triggers that are listening for changes to the `Recipe` relation. For example, if we have a trigger that creates embeddings based on the recipe's ingredients, we could have a trigger that listens for changes to the `Recipe` relation and updates the embeddings index with new embeddings. The other trigger, `search`, will listen for changes to the `Recipe` relation and update the recipe index with new recipes.

### Search

The `search` trigger will take a user's ingredients and return a list of recipes that they can make. This trigger has two routes: one for when a user submits a query (i.e., a list of ingredients they have) to retrieve relevant recipes in the recipe index, and one for updating the recipe index when a new recipe is added to the `Recipe` relation.

 We'll use the `cohere` library to create embeddings for each recipe's ingredients and the `faiss` library to create an index of the embeddings. We only show the trigger code here (`routes`, `setUp`, `infer`, `fit`), but you can find the full code that includes helper methods in `triggers/search.py`.

```py title="triggers/search.py"
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
                infer=None, # (1)!
                fit=self.addRecipeToIndex, # (2)!
            ),
            motion.Route(
                relation="Query",
                key="ingredients",
                infer=self.findNearestRecipes, # (3)!
                fit=None, # (4)!
            ),
        ]

    def setUp(self, cursor: motion.Cursor) -> Dict:
        # Set up the recipe index and cohere client
        co = cohere.Client(os.environ["COHERE_API_KEY"])
        recipe_index = faiss.IndexFlatIP(4096)
        recipe_index_to_id: Dict[int, str] = {}

        ingredients_df = cursor.sql("SELECT identifier, ingredients FROM Recipe")

        if len(ingredients_df) > 0: # (5)!
            ingredients_list = ingredients_df["ingredients"].tolist()
            recipe_ids = ingredients_df["identifier"].tolist()

            recipe_index, recipe_index_to_id = self._populateRecipeIndex(
                co,
                ingredients_list,
                recipe_ids,
                recipe_index,
                recipe_index_to_id,
            ) # (6)!

        return {
            "cohere": co,
            "recipe_stream": [],
            "recipe_index": recipe_index,
            "recipe_index_to_id": recipe_index_to_id,
        }

    def addRecipeToIndex(
        self,
        cursor: motion.Cursor,
        trigger_context: motion.TriggerElement, # (7)!
        infer_context: typing.Any, # (8)!
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

        return new_state # (9)!

    def findNearestRecipes(
        self, cursor: motion.Cursor, trigger_context: motion.TriggerElement # (10)!
    ) -> None:
        # Find the nearest recipes for the query
        ingredients = trigger_context.value # (11)!
        response = self.state["cohere"].embed(texts=[ingredients])
        embedding = np.array(response.embeddings[0]).reshape(1, -1)

        scores, recipe_ids = self._searchIndex(embedding) # (12)!
        for score, recipe_id in zip(scores, recipe_ids):
            duplicate_id = cursor.duplicate(
                relation=trigger_context.relation, identifier=trigger_context.identifier
            )
            cursor.set(
                relation=trigger_context.relation,
                identifier=duplicate_id,
                key_values={"recipe_id": recipe_id, "recipe_score": score},
            ) # (13)!
```

1. We don't need to infer anything when a new recipe is added
2. We need to update the recipe index when a new recipe is added (this is a state change)
3. We need to infer the most similar recipes when a user submits a query
4. We don't need to update trigger state when a user submits a query
5. If there are already recipes in the database, we need to populate the recipe index with them. This is useful when restarting an application with a previous session ID, where there are already recipes in the database.
6. `self._populateRecipeIndex` is a helper method that takes a list of ingredients and returns a new recipe index and recipe index to id mapping. We use the `cohere` library to create embeddings for each recipe's ingredients and the `faiss` library to create an index of the embeddings.
7. The `trigger_context` is a `TriggerElement` object that contains the identifier and value of the element that triggered the trigger. In this case, the `trigger_context` is the `Recipe` relation element that was added to the database. The relation will be `Recipe`, the key will be `ingredients`, and the `identifier` and `value` will be the identifier and ingredients of the recipe.
8. The `infer_context` is the context that was returned by the `infer` method of this trigger. In this case, there is no `infer` method defined in the route, so the `infer_context` is `None`.
9. We return the new state of the trigger. In this case, we return the new recipe index and recipe index to id mapping (for retrieving the recipe id from the recipe index).
10. In this case, the `trigger_context` is the `Query` relation element that was added to the data store. The relation will be `Query`, the key will be `ingredients`, and the `identifier` and `value` will be the identifier and ingredients of the query. Note that `Query` ingredients are different from `Recipe` ingredients, since they are two different relations!
11. The ingredients of the query are the `value` of the `trigger_context`, since the `key` of the route is `ingredients`.
12. `self._searchIndex` is a helper method that takes an embedding and returns the most similar recipes in the index. We use the `faiss` library to search the index for the most similar recipes.
13. For each query, we want to store multiple similar recipes. Thus, we duplicate the query record and set the `recipe_id` and `recipe_score` for each duplicate. The `recipe_id` is the identifier of the recipe in the `Recipe` relation, and the `recipe_score` is the similarity score between the query and the recipe.

!!! info

    To retrieve the results of triggers, it's important to add the results back to the data store. This is done via the `cursor` object. If the results are not added back to the data store, the client (i.e., dashboard) will not be able to retrieve them.

!!! warning

    Remember that `fit` methods (i.e., `addRecipeToIndex`) must return a `Dict` of the new state of the trigger, and `infer` methods (i.e., `findNearestRecipes`) cannot modify state.

After writing the trigger code, we define the triggers in `triggers/__init__.py`:

```python title="triggers/__init__.py"
from triggers.search import SearchRecipe
from triggers.scrape import ScrapeBonAppetit

__all__ = ["SearchRecipe", "ScrapeBonAppetit"]
```

## `mconfig.py`

To define the Motion configuration, we make sure our `mconfig.py` file points to our relations and triggers. The current `mconfig.py` file is:

```python title="mconfig.py"
from schemas import Query, Recipe
from triggers import ScrapeBonAppetit, SearchRecipe

MCONFIG = {
    "application": {
        "name": "cooking",
        "author": "shreyashankar", # Your name here
        "version": "0.1",
    },
    "relations": [Recipe, Query],
    "triggers": [SearchRecipe, ScrapeBonAppetit],
    "trigger_params": {SearchRecipe: {"num_recipe_results": 10}}, # (1)!
    "checkpoint": "0 * * * *", # (2)!
}
```

1. We set the number of recipe results to 10. This means that for each query, we will return the 10 most similar recipes. You can change this number to any integer.
2. We set the checkpoint to run every hour, meaning that the data store will be saved to disk every hour. This is useful if you want to restart the application with a previous session ID. Note that trigger state is not saved to disk, so it's important to define the `setUp` trigger methods to populate trigger state given what might already be in the data store.

### Exercise

Note that we hard-coded the interval of when to add recipes to the index in `addRecipeToIndex`. This means that we will only add recipes to the index every 20 recipes. How might we turn this into a parameter that can be set in `mconfig.py`?

(1) Add a parameter to `mconfig.py` that sets the interval of when to add recipes to the index.

```python title="mconfig.py" hl_lines="12 13 14"
from schemas import Query, Recipe
from triggers import ScrapeBonAppetit, SearchRecipe

MCONFIG = {
    "application": {
        "name": "cooking",
        "author": "shreyashankar",  # Your name here
        "version": "0.1",
    },
    "relations": [Recipe, Query],
    "triggers": [SearchRecipe, ScrapeBonAppetit],
    "trigger_params": {
        SearchRecipe: {"num_recipe_results": 10, "num_recipe_interval": 20}
    },
    "checkpoint": "0 * * * *",  # (2)!
}

```

(2) Use the parameter in `addRecipeToIndex`

```python title="triggers/search.py" hl_lines="18"

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
        if len(recipe_stream) % self.params["num_recipe_interval"] == 0:
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

```

## Testing

In the `test.py` file, we can test our application by adding a query and seeing if the results are returned. First, we create a test connection:

```python
from mconfig import MCONFIG
import motion

connection = motion.test(
    MCONFIG,
    wait_for_triggers=["ScrapeBonAppetit"], # (1)!
    motion_logging_level="INFO",
    session_id="EXAMPLE_SESSION_ID",  # Can comment this out to generate a new session ID
)
```

1. We wait for the `ScrapeBonAppetit` trigger to finish scraping the recipes. This is because we want to make sure that the index is populated before we issue a query.

Then, we add a query and see if the results are returned:

```python
ingredients = "pasta;tomatoes;garlic;cheese"
new_id = connection.set( # (1)!
    relation="Query",
    identifier="",
    key_values={"ingredients": ingredients},
)
recipe_ids_and_scores = connection.get( # (2)!
    relation="Query",
    identifier=new_id,
    keys=["recipe_id", "recipe_score"],
    include_derived=True,
    as_df=True,
)
recipe_ids_and_titles = connection.mget( # (3)!
    relation="Recipe",
    identifiers=list(recipe_ids_and_scores["recipe_id"].values),
    keys=["title", "recipe_url"],
    as_df=True,
).rename(columns={"identifier": "recipe_id"})
result = recipe_ids_and_scores.merge(recipe_ids_and_titles, on="recipe_id")

print(f"Ingredient list: {ingredients}")
print(f"Response: {result}")
```

1. The `identifier` is set to `""` because we want Motion to generate a unique identifier for us. Motion coordinates the triggers to run.
2. To get the `recipe_id` and `recipe_score` for the query, we must the `get` method. We set `include_derived=True` to include records that are derived from the original record, because we store multiple similar recipes for each query.
3. We need to get the recipe title and url from the recipe relation for each recipe id. We use the `mget` method to get multiple records at once.

The output should look something like this (it will take some time to run for the first time):


```bash
$ python test.py

Ingredient list: pasta;tomatoes;garlic;cheese
Response:                               recipe_id  recipe_score  ...                                            title                                       
recipe_url
0  34868e77-df2a-44c8-80c3-6822c518fa08      0.558999  ...              Baked Tomato Feta Pasta With a Kick  
https://www.bonappetit.com/recipe/spicy-feta-p...
1  e4ebc85e-9a9c-429b-a4c5-ba816b63cdb7      0.531800  ...                           Best Eggplant Parmesan  
https://www.bonappetit.com/recipe/bas-best-egg...
2  3d4b51ad-7aa5-40e2-9d37-03006cf71cd0      0.526027  ...                             BA's Best Baked Ziti       
https://www.bonappetit.com/recipe/baked-ziti
3  80162e20-f1d9-45f7-81c0-a1e1bfe94478      0.524651  ...                 Spaghetti With Eggplant Caponata  
https://www.bonappetit.com/recipe/spaghetti-wi...
4  c7c8a28d-a3f7-4593-b56c-adb8ffacb4b9      0.524256  ...                             Simple Sandwich Loaf  
https://www.bonappetit.com/recipe/pullman-sand...
5  d47d3d19-7507-4139-8d3c-94c1ff5ef910      0.517231  ...                                   Weeknight Ragù   
https://www.bonappetit.com/recipe/weeknight-ragu
6  72dc9c06-c5b8-4a76-adbe-814bf46a3939      0.516418  ...              Grated Tomato and Miso-Butter Pasta  
https://www.bonappetit.com/recipe/grated-tomat...
7  51c5acbe-aa71-4ae4-9134-781d77973747      0.514711  ...                               One-Pot Puttanesca  
https://www.bonappetit.com/recipe/one-pot-putt...
8  12007fc9-6a56-4054-97a4-a6184ee31de0      0.511173  ...                     Mamá Rosa’s Lasagna de Carne  
https://www.bonappetit.com/recipe/mama-rosas-l...
9  72179df1-f426-4e63-b6fb-25a84ba85256      0.510625  ...  Creamy Lemon Pappardelle With Crispy Prosciutto  
https://www.bonappetit.com/recipe/creamy-lemon...

[10 rows x 5 columns]
```

## Running the Dashboard

In this example project, we have also included a dashboard that allows you to query the recipe index. The dashboard uses [Streamlit](https://streamlit.io/) under the hood. To run the dashboard, run the following command from your `cooking` directory:

```bash
$ streamlit run dashboard.py
```

The dashboard may take some time to load initial trigger state and run initial triggers (i.e., scraping) for the first time, but it should be much faster after that. You can then query the recipe index by entering a list of ingredients. For example, you can enter `pasta;tomatoes;garlic;cheese` to get the top 10 recipes that contain those ingredients. You can try other queries!

Here's a screenshot of the dashboard:

![Dashboard Screenshot](images/starter-dashboard.png)