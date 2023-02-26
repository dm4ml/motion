import motion
import streamlit as st

import components


@st.cache_resource
def setup_database():
    # Create store and add triggers
    store = motion.get_or_create_store(
        "fashion",
    )
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
# st.subheader("Fashion Search")
query = st.text_input("What to wear to")

if query:
    query_id = store.getNewId("query")
    with st.spinner("Fetching results..."):
        store.setMany(
            "query",
            id=None,
            key_values={
                "query": query,
                "src": components.QuerySource.ONLINE,
                "query_id": query_id,
            },
        )
        results = store.con.execute(
            "SELECT fashion.query.query, fashion.query.text_suggestion, fashion.catalog.permalink, fashion.catalog.img_url, fashion.query.img_score FROM fashion.query JOIN fashion.catalog ON fashion.query.img_id = fashion.catalog.id WHERE fashion.query.img_id IS NOT NULL AND fashion.query.query_id = ? ORDER BY fashion.query.img_score ASC",
            (query_id,),
        ).fetchdf()

        deduplicated_results = results[
            ["permalink", "img_url"]
        ].drop_duplicates()

        st_cols = st.columns(3)
        col_idx = 0

        for _, row in deduplicated_results.iterrows():
            st_cols[col_idx].image(row["img_url"])
            st_cols[col_idx].markdown(
                f'[Link]({"https://www.everlane.com/products/" + row["permalink"]})'
            )
            col_idx = (col_idx + 1) % 3
