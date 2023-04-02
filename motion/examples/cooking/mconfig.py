from schemas import Query, Recipe
from triggers import ScrapeBonAppetit, SearchRecipe

MCONFIG = {
    "application": {
        "name": "cooking",
        "author": "{1}",
        "version": "0.1",
    },
    "relations": [Recipe, Query],
    "triggers": [SearchRecipe, ScrapeBonAppetit],
    "trigger_params": {SearchRecipe: {"num_recipe_results": 10}},
    "checkpoint": "0 * * * *",
}
