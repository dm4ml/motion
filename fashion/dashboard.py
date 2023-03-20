import streamlit as st

import motion
from mconfig import MCONFIG
from schemas import QuerySource


@st.cache_resource
def setup_database():
    store = motion.init(MCONFIG)
    return store


store = setup_database()


@st.cache_data(show_spinner="Fetching results...")
def run_query(query):
    cursor = store.cursor()
    query_id = cursor.getNewId("query")

    created_id = cursor.set(
        "query",
        identifier=None,
        key_values={
            "query": query,
            "src": QuerySource.ONLINE,
            "query_id": query_id,
        },
    )

    # Retrieve the results and get the lowest cosine similarity
    # (i.e., best match) for each img_id
    results = cursor.get(
        "query",
        identifier=created_id,
        keys=["identifier", "text_suggestion", "img_id", "img_score"],
        include_derived=True,
        as_df=True,
    )
    results = results.loc[
        results.groupby("img_id").img_score.idxmin()
    ].reset_index(drop=True)
    results.rename(columns={"identifier": "qid"}, inplace=True)

    # Retrieve the image url and permalink for each img_id
    image_results = cursor.mget(
        "catalog",
        identifiers=results["img_id"].values,
        keys=["img_url", "permalink"],
        as_df=True,
    ).merge(results, left_on="identifier", right_on="img_id")

    return image_results


query = st.text_input("What to wear to")

if query:
    image_results = run_query(query)

    #  with st.spinner("Fetching results..."):
    st_cols = st.columns(3)
    col_idx = 0

    for _, row in image_results.iterrows():
        st_cols[col_idx].image(row["img_url"])
        st_cols[col_idx].markdown(
            f'{row["text_suggestion"]} | [Link]({row["permalink"]})'
        )
        st_cols[col_idx].button(
            key="like_" + str(row["img_id"]),
            label="Like this",
            on_click=lambda x: store.cursor().set(
                "query",
                identifier=x,
                key_values={"feedback": True},
            ),
            args=(row["qid"],),
            type="primary",
            use_container_width=True,
        )
        col_idx = (col_idx + 1) % 3
