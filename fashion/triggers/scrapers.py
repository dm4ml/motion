import asyncio
import logging
import json
import pandas as pd
import requests

from fashion.triggers.utils import async_download_image
from fp.fp import FreeProxy

from bs4 import BeautifulSoup
from fashion.schemas import Retailer


def scrape_everlane(cursor, triggered_by):
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
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36",
        # "referer": "https://finance.yahoo.com/",
        # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        # "Accept-Language": "en-US,en;q=0.5",
        # "Accept-Encoding": "gzip, deflate",
        # "Connection": "keep-alive",
        # "Upgrade-Insecure-Requests": "1",
        # "Sec-Fetch-Dest": "document",
        # "Sec-Fetch-Mode": "navigate",
        # "Sec-Fetch-Site": "none",
        # "Sec-Fetch-User": "?1",
        # "Cache-Control": "max-age=0",
    }
    product_info = []
    for url in urls:
        # try:
        #     proxy = FreeProxy(
        #         country_id=["US"], rand=True, https=True, elite=True
        #     ).get()
        # except Exception as e:
        #     print(
        #         f"Failed to get proxy with error {e}. Trying without proxy."
        #     )
        #     proxy = None

        try:
            r = requests.get(url=url, headers=headers)
            if r.status_code != 200:
                print(
                    f"Request failed with status code {r.status_code}. Skipping url {url}."
                )
                continue
        except Exception as e:
            print(f"Request failed with error {e}. Skipping url {url}.")
            continue

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
    print(f"Found {len(df)} unique products.")
    df = df.head(100)

    # Filter out products that are already in the store
    existing_img_urls = cursor.sql("SELECT img_url FROM fashion.catalog")[
        "img_url"
    ].values
    df = df[~df["img_url"].isin(existing_img_urls)]
    print(f"Found {len(df)} new products.")

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
        cursor.set("catalog", identifier=new_id, key_values=product)
