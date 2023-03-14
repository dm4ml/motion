import asyncio
import logging
import json
import pandas as pd
import requests

from fashion.triggers.utils import async_download_image

from bs4 import BeautifulSoup
from fashion.schemas import Retailer


def scrape_everlane_sale(cursor, k=20):
    # Scrape the catalog and add the images to the store
    urls = [
        # "https://www.everlane.com/collections/womens-sale-2",
        "https://www.everlane.com/collections/womens-all-tops",
        "https://www.everlane.com/collections/womens-tees",
        "https://www.everlane.com/collections/womens-sweaters",
        "https://www.everlane.com/collections/womens-sweatshirts",
        "https://www.everlane.com/collections/womens-bodysuits",
        "https://www.everlane.com/collections/womens-jeans",
        "https://www.everlane.com/collections/womens-bottoms",
        "https://www.everlane.com/collections/womens-skirts-shorts",
        "https://www.everlane.com/collections/womens-dresses",
        "https://www.everlane.com/collections/womens-outerwear",
        "https://www.everlane.com/collections/womens-underwear",
        "https://www.everlane.com/collections/womens-perform",
        "https://www.everlane.com/collections/swimwear",
        "https://www.everlane.com/collections/womens-shoes",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"
    }
    product_info = []
    for url in urls:
        r = requests.get(url=url, headers=headers)

        soup = BeautifulSoup(r.content, "html5lib")

        res = soup.find("script", attrs={"id": "__NEXT_DATA__"})
        products = json.loads(res.contents[0])["props"]["pageProps"][
            "fallbackData"
        ]["products"]

        for product in products:
            img_url = product["albums"]["square"][0]["src"]
            img_name = product["displayName"]
            permalink = product["permalink"]
            product_info.append(
                {
                    "img_url": img_url,
                    "img_name": img_name,
                    "permalink": "https://www.everlane.com/products/"
                    + permalink,
                }
            )

    # Delete duplicates
    df = pd.DataFrame(product_info)
    df = (
        df.drop_duplicates(subset=["img_url"])
        .sample(frac=1)
        .reset_index(drop=True)
    )
    logging.info(f"Found {len(df)} unique products.")
    df = df.head(k)

    # Get blobs from the images
    img_urls, contents = asyncio.run(
        async_download_image(df["img_url"].values)
    )
    img_url_to_content = dict(zip(img_urls, contents))

    for _, product_row in df.iterrows():
        if product_row["img_url"] not in img_url_to_content:
            continue

        new_id = cursor.getNewId("catalog")
        product = product_row.to_dict()
        product.update(
            {
                "retailer": Retailer.EVERLANE,
                "img_blob": img_url_to_content[product_row["img_url"]],
            }
        )
        cursor.set("catalog", id=new_id, key_values=product)
