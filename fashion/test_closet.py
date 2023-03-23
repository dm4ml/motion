import motion
import os

from fashion.schemas import QuerySource
from mconfig import MCONFIG
from rich import print

from PIL import Image
from io import BytesIO

# Test that for simple queries, the results make some sense


def test_add_item_to_closet(strength: float):
    test_config = motion.update_params(
        MCONFIG, {"ExtractOutfit": {"strength": strength}}
    )

    connection = motion.test(
        test_config,
        wait_for_triggers=["scrape_everlane"],
        motion_logging_level="INFO",
        # disable_cron_triggers=True,
    )
    print(connection.session_id)

    images = [
        "jumpsuit.JPG",
        "jumpsuit2.JPG",
    ]

    for image in images:
        blob = open(os.path.join("images", image), "rb").read()

        created_id = connection.set(
            namespace="closet",
            identifier=None,
            key_values={
                "username": "shreya",
                "img_blob": blob,
            },
        )

        # Retrieve the results
        results = connection.get(
            namespace="closet",
            identifier=created_id,
            keys=["identifier", "sd_img_blob", "catalog_img_score"],
            include_derived=True,
            as_df=True,
        ).sort_values("catalog_img_score", ascending=True)
        # Convert sd_image_blob to image

        img = Image.open(
            BytesIO(results["sd_img_blob"].values[0]),
        )
        img.save(
            os.path.join("images", f"sd_generated_{image[:-4]}.png"),
            format=img.format,
        )

    connection.close(wait=False)


for i in [0.2]:
    test_add_item_to_closet(i)
