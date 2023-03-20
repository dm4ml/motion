import motion
from fashion.schemas import QuerySource
from mconfig import MCONFIG


def test_simple_set_get():
    connection = motion.test(MCONFIG, wait_for_triggers=["scrape_everlane"])
    created_id = connection.set(
        namespace="query",
        identifier=None,
        key_values={
            "query": "the beach in Maui",
            "src": QuerySource.ONLINE,
        },
    )

    # Retrieve the results
    results = connection.get(
        namespace="query",
        identifier=created_id,
        keys=["identifier", "text_suggestion", "img_id", "img_score"],
        include_derived=True,
        as_df=True,
    )
    print(results)


test_simple_set_get()
