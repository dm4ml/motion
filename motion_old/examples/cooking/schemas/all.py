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
    image_url: str
    recipe_url: str


class Query(motion.Schema):
    username: str
    ingredients: str
    recipe_id: str
    recipe_score: float
    feedback: bool
