import motion
import os

from fashion.schemas import QuerySource
from mconfig import MCONFIG
from rich import print

# Test that for simple queries, the results make some sense


def test_add_item_to_closet():
    connection = motion.test(
        MCONFIG,
        wait_for_triggers=["scrape_everlane"],
        motion_logging_level="INFO",
    )

    images = [
        "jumpsuit.JPG",
        "jumpsuit2.JPG",
    ]

    for image in images:
        # Turn image into blob

        created_id = connection.set(
            namespace="closet",
            identifier=None,
            key_values={
                "username": "shreya",
                "img_path": os.path.join("images", image),
            },
        )

        # Retrieve the results
        results = connection.get(
            namespace="closet",
            identifier=created_id,
            keys=["identifier", "img_embedding"],
            include_derived=True,
            as_df=True,
        )
        print(f"Results for image '{image}': {results}")

    connection.close(wait=False)


test_add_item_to_closet()
