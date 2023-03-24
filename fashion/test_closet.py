import motion
import os
import wandb

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
        session_id="620f3434-116b-4a17-9ecd-e0808d399bac",
    )
    print(connection.session_id)

    wandb.init(
        # set the wandb project where this run will be logged
        project="fashion-testcloset",
        # track hyperparameters and run metadata
        config={
            "strength": strength,
            "session_id": connection.session_id,
        },
    )

    images = [
        "jumpsuit.JPG",
        "jumpsuit2.JPG",
    ]

    for image in images:
        blob = open(os.path.join("images", image), "rb").read()

        created_id = connection.set(
            relation="closet",
            identifier=None,
            key_values={
                "username": "shreya",
                "img_blob": blob,
            },
        )

        # Retrieve the results
        results = connection.get(
            relation="closet",
            identifier=created_id,
            keys=["identifier", "sd_img_blob", "catalog_img_score"],
            include_derived=True,
            as_df=True,
        ).sort_values("catalog_img_score", ascending=True)
        # Convert sd_image_blob to image

        img = Image.open(
            BytesIO(results["sd_img_blob"].values[0]),
        )
        # img.save(
        #     os.path.join("images", f"sd_generated_{image[:-4]}.png"),
        #     format=img.format,
        # )
        wandb.log(
            {
                "sd_generated_{image[:-4]}": wandb.Image(img),
                "image_scores": wandb.Table(
                    dataframe=results[["identifier", "catalog_img_score"]]
                ),
            }
        )

    connection.close(wait=False)


for i in [0.2]:
    test_add_item_to_closet(i)
