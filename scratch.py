import requests
from bs4 import BeautifulSoup

from rich import print
import pandas as pd

import json


def scrape_everlane_sale():
    # Scrape the catalog and add the images to the store
    urls = [
        "https://www.everlane.com/collections/womens-sale-2",
        "https://www.everlane.com/collections/mens-sale-2",
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
                    "permalink": permalink,
                }
            )

    df = pd.DataFrame(product_info)
    df = df.drop_duplicates(subset=["img_url"])
    print(len(df))


scrape_everlane_sale()
