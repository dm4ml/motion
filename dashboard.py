import motion
import streamlit as st

import components


@st.cache_resource
def setup_database():
    # Create store and add triggers
    store = motion.get_store("fashion", create=True, memory=False)
    store.addNamespace("query", components.QuerySchema)
    store.addNamespace("catalog", components.CatalogSchema)

    store.addTrigger(
        name="suggest_idea",
        keys=["query.query"],
        trigger=components.SuggestIdea,
    )
    store.addTrigger(
        name="retrieval",
        keys=["catalog.img_url", "query.text_suggestion"],
        trigger=components.Retrieval,
    )

    # Add the catalog
    components.scrape_everlane_sale(store)

    return store


def make_grid(cols, rows):
    grid = [0] * cols
    for i in range(cols):
        with st.container():
            grid[i] = st.columns(rows)
    return grid


store = setup_database()
query = st.text_input("What to wear to")

if query:
    query_id = store.getNewId("query")
    with st.spinner("Fetching results..."):
        created_id = store.set(
            "query",
            id=None,
            key_values={
                "query": query,
                "src": components.QuerySource.ONLINE,
                "query_id": query_id,
            },
        )

        # Retrieve the results and get the lowest cosine similarity
        # (i.e., best match) for each img_id
        results = (
            store.get(
                "query",
                id=created_id,
                keys=["img_id", "img_score"],
                include_derived=True,
            )
            .groupby("img_id")
            .min()
            .reset_index()
        )
        # Retrieve the image url and permalink for each img_id
        image_results = store.mget(
            "catalog",
            ids=results["img_id"].values,
            keys=["img_url", "permalink"],
        )

        st_cols = st.columns(3)
        col_idx = 0

        for _, row in image_results.iterrows():
            st_cols[col_idx].image(row["img_url"])
            st_cols[col_idx].markdown(
                f'[Link]({"https://www.everlane.com/products/" + row["permalink"]})'
            )
            col_idx = (col_idx + 1) % 3
