import pandas as pd
import streamlit as st
from mconfig import MCONFIG

import motion


@st.cache_resource
def setup_database() -> motion.ClientConnection:
    connection = motion.test(
        MCONFIG,
        wait_for_triggers=["ScrapeBonAppetit"],
        # motion_logging_level="WARNING",  # Can be "INFO" or "DEBUG" for more verbose logging
        session_id="EXAMPLE_SESSION_ID",
    )
    return connection


connection = setup_database()


@st.cache_data(show_spinner="Fetching results...")
def run_query(ingredients: str) -> pd.DataFrame:
    # Retrieve the results and get the lowest cosine similarity
    # (i.e., best match) for each img_id
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
        keys=["title", "recipe_url", "image_url"],
        as_df=True,
    ).rename(columns={"identifier": "recipe_id"})
    results = recipe_ids_and_scores.merge(recipe_ids_and_titles, on="recipe_id")

    return results


ingredients = st.text_input("Type a list of ingredients you have in your fridge")

if ingredients:
    image_results = run_query(ingredients)

    #  with st.spinner("Fetching results..."):
    st_cols = st.columns(3)
    col_idx = 0

    for _, row in image_results.iterrows():
        st_cols[col_idx].image(row["image_url"])
        st_cols[col_idx].markdown(f'{row["title"]} | [Link]({row["recipe_url"]})')
        col_idx = (col_idx + 1) % 3
