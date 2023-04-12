from mconfig import MCONFIG
from rich import print

import motion

# Test that for simple queries, the results make some sense


def test_scrape() -> None:
    connection = motion.test(
        MCONFIG,
        wait_for_triggers=["ScrapeBonAppetit"],
        motion_logging_level="INFO",  # Can be "INFO" or "DEBUG" for more verbose logging
        session_id="EXAMPLE_SESSION_ID",  # Can comment this out to generate a new session ID
    )
    print(f"Session ID: {connection.session_id}")

    # Must specify kw for every arg in .set and .get
    ingredients = "pasta;tomatoes;garlic;cheese"
    new_id = connection.set(
        relation="Query",
        identifier="",
        key_values={"ingredients": ingredients},
    )
    recipe_ids_and_scores = connection.get(
        relation="Query",
        identifier=new_id,
        keys=["recipe_id", "recipe_score"],
        include_derived=True,
        as_df=True,
    )
    recipe_ids_and_titles = connection.mget(
        relation="Recipe",
        identifiers=list(recipe_ids_and_scores["recipe_id"].values),
        keys=["title", "recipe_url"],
        as_df=True,
    ).rename(columns={"identifier": "recipe_id"})
    result = recipe_ids_and_scores.merge(recipe_ids_and_titles, on="recipe_id")

    print(f"Ingredient list: {ingredients}")
    print(f"Response: {result}")

    connection.checkpoint()


test_scrape()
