import motion
import streamlit as st

import components


@st.cache_resource
def setup_database():
    # Create store and add triggers
    store = motion.get_store("fashion", create=True, memory=True)
    store.addNamespace("query", components.QuerySchema)
    store.addNamespace("catalog", components.CatalogSchema)

    store.addTrigger(
        name="suggest_idea",
        keys=["query.query"],
        trigger=components.SuggestIdea,
    )
    store.addTrigger(
        name="retrieval",
        keys=["catalog.img_blob", "query.text_suggestion", "query.feedback"],
        trigger=components.Retrieval,
    )

    # Add the catalog
    components.scrape_everlane_sale(store, k=20)

    return store


@st.cache_data(show_spinner="Fetching results...")
def run_query(query):
    query_id = store.getNewId("query")

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
    results = store.get(
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
    image_results = store.mget(
        "catalog",
        ids=results["img_id"].values,
        keys=["img_url", "permalink"],
    ).merge(results, left_on="id", right_on="img_id")

    return image_results


store = setup_database()
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
            on_click=lambda x: store.set(
                "query",
                id=x,
                key_values={"feedback": True},
            ),
            args=(row["qid"],),
            type="primary",
            use_container_width=True,
        )
        col_idx = (col_idx + 1) % 3
