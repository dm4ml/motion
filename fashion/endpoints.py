import logging
import motion
import os

import modal

import time

from mconfig import MCONFIG

image = modal.Image.debian_slim().pip_install_from_requirements(
    "requirements.txt"
)
volume = modal.SharedVolume().persist(MCONFIG["application"]["name"])
CACHE_DIR = "/cache"

stub = modal.Stub("example-spin", image=image)


@stub.function(
    shared_volumes={CACHE_DIR: volume},
    mounts=modal.create_package_mounts(["motion", "fashion"]),
    secret=modal.Secret(
        {
            "COHERE_API_KEY": os.environ["COHERE_API_KEY"],
            "MOTION_HOME": CACHE_DIR,
        }
    ),
    timeout=100,
)
def initStore(config):
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    store = motion.init(config)
    return store


@stub.local_entrypoint
def main():
    from mconfig import MCONFIG

    store = initStore.call(MCONFIG)
    time.sleep(200)

    # import requests

    # print(requests.get("https://www.everlane.com/collections/womens-all-tops"))


image = (
    modal.Image.debian_slim()
    .pip_install_from_requirements("requirements.txt")
    .pip_install(["tensorflow-data-validation"])
)
stub = modal.Stub(image=image)
