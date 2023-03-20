import motion

from fashion.schemas import QuerySource
from mconfig import MCONFIG

# Test that for simple queries, the results make some sense


def test_place_queries():
    connection = motion.test(MCONFIG, wait_for_triggers=["scrape_everlane"])

    place_queries = [
        "the beach in Maui",
        "hiking in the mountains",
        "a nice restaurant in San Francisco",
    ]

    for query in place_queries:
        created_id = connection.set(
            namespace="query",
            identifier=None,
            key_values={
                "query": query,
                "src": QuerySource.ONLINE,
            },
        )

        # Retrieve the results
        results = (
            connection.get(
                namespace="query",
                identifier=created_id,
                keys=["text_suggestion"],
                include_derived=True,
                as_df=True,
            )["text_suggestion"]
            .drop_duplicates()
            .values
        )
        print(f"Results for query '{query}': {results}")

    connection.close(wait=False)


test_place_queries()
