import motion


class RecipeSource(motion.MEnum):
    BONAPPETIT = "Bon Appetit"


class Recipe(motion.Schema):
    src: RecipeSource
    title: str
    description: str
    ingredients: str
    instructions: str
    image_url: str
    recipe_url: str
    image_bytes: bytes


class Query(motion.Schema):
    username: str
    ingredients: str
    recipe_id: str
    recipe_score: float
    feedback: bool
