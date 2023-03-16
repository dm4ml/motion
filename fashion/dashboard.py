import streamlit as st

import motion
from mconfig import mconfig
from schemas import QuerySource


@st.cache_resource
def setup_database():
    store = motion.init(mconfig)
    return store


store = setup_database()


@st.cache_data(show_spinner="Fetching results...")
def run_query(query):
    cursor = store.cursor()
    query_id = cursor.getNewId("query")

    created_id = cursor.set(
        "query",
        id=None,
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
        id=created_id,
        keys=["id", "text_suggestion", "img_id", "img_score"],
        include_derived=True,
    )
    results = results.loc[
        results.groupby("img_id").img_score.idxmin()
    ].reset_index(drop=True)
    results.rename(columns={"id": "qid"}, inplace=True)

    # Retrieve the image url and permalink for each img_id
    image_results = cursor.mget(
        "catalog",
        ids=results["img_id"].values,
        keys=["img_url", "permalink"],
    ).merge(results, left_on="id", right_on="img_id")

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
                id=x,
                key_values={"feedback": True},
            ),
            args=(row["qid"],),
            type="primary",
            use_container_width=True,
        )
        col_idx = (col_idx + 1) % 3
