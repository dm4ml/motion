import motion

from fashion.schemas import QuerySource
from mconfig import MCONFIG

# Test that for simple queries, the results make some sense


def test_place_queries():
    connection = motion.test(
        MCONFIG,
        # wait_for_triggers=["scrape_everlane"],
        disable_cron_triggers=True,
        motion_logging_level="INFO",
        session_id="567161e8-53b4-47e5-940b-5ca4f2fa9677",
    )
    print(connection.session_id)

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
        results = connection.get(
            namespace="query",
            identifier=created_id,
            keys=[
                "identifier",
                "text_suggestion",
                "catalog_img_id",
                "catalog_img_score",
            ],
            include_derived=True,
            as_df=True,
        )

        image_url_results = connection.mget(
            namespace="catalog",
            identifiers=list(results["catalog_img_id"].values),
            keys=["img_url", "permalink"],
            as_df=True,
        )

        print(
            f"Results for query '{query}': {results['text_suggestion'].drop_duplicates() .values}"
        )
        print(f"Image URLs for query '{query}': {image_url_results}")

    connection.close(wait=False)


test_place_queries()
