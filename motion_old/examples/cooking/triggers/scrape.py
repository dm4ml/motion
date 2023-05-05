import asyncio
import json
from typing import Any, Dict, List

import aiohttp
import pandas as pd
import requests
from bs4 import BeautifulSoup
from schemas import RecipeSource

import motion


class ScrapeBonAppetit(motion.Trigger):
    def routes(self) -> List[motion.Route]:
        return [
            motion.Route(
                relation="",
                key="0 * * * *",
                infer=self.scrape,
                fit=None,
            )
        ]

    def setUp(self, cursor: motion.Cursor) -> Dict:
        return {}

    def scrape(
        self, cursor: motion.Cursor, trigger_context: motion.TriggerElement
    ) -> None:
        # Set lower time bound to avoid scraping recipes we've already scraped
        lower_bound = cursor.sql(
            "SELECT MAX(create_at) AS lower_bound FROM Recipe WHERE src='Bon Appetit'"
        )["lower_bound"].values[0]
        if pd.isnull(lower_bound):
            lower_bound = "2019-01-01"
        else:
            lower_bound = str(lower_bound)

        url_list = asyncio.run(
            fetch_all_urls("https://www.bonappetit.com/sitemap.xml", lower_bound)
        )

        print(f"Found {len(url_list)} recipes.")

        recipes = asyncio.run(scrape_all_recipes(url_list))
        print(f"Scraped {len(recipes)} recipes.")

        for recipe in recipes:
            recipe.update({"src": RecipeSource.BONAPPETIT})
            cursor.set(relation="Recipe", identifier="", key_values=recipe)


async def async_scrape_recipe(url: str, session: aiohttp.ClientSession) -> Any:
    try:
        async with session.get(url) as response:
            if response.status != 200:
                print(
                    f"Failed to fetch the webpage {url}. Status code: {response.status}"
                )
                return None

            content = await response.content.read()
            soup = BeautifulSoup(content, "html.parser")

            # Extract json ld data
            data = json.loads(soup.find("script", type="application/ld+json").string)
            title = data["name"]
            description = data["description"]
            ingredients = data["recipeIngredient"]
            instructions = [i["text"] for i in data["recipeInstructions"]]
            image_url = data["image"][0]
            if not image_url.startswith("http"):
                raise Exception("Image url is not valid.")

            recipe = {
                "title": title,
                "description": description,
                "ingredients": ";".join(ingredients),
                "instructions": ";".join(instructions),
                "image_url": image_url,
                "recipe_url": url,
            }

            return recipe
    except Exception as e:
        print(f"Failed to scrape recipe: {url} with error {e}")


async def scrape_all_recipes(url_list: List[str]) -> Any:
    async with aiohttp.ClientSession() as session:
        tasks = [async_scrape_recipe(url, session) for url in url_list]

        ret = await asyncio.gather(*tasks)
        ret = [x for x in ret if x is not None]

    return ret


async def fetch_sitemap_urls(sitemap_url: str, session: aiohttp.ClientSession) -> Any:
    try:
        async with session.get(sitemap_url) as response:
            if response.status != 200:
                print(
                    f"Failed to fetch the sitemap {sitemap_url}. Status code: {response.status}"
                )
                return []

            content = await response.content.read()

            soup = BeautifulSoup(content, "xml")
            urls = [loc.text for loc in soup.find_all("loc")]

        return urls
    except Exception as e:
        print(f"Failed to fetch sitemap: {sitemap_url} with error {e}")


async def fetch_all_urls(sitemap_url: str, date_threshold: str) -> Any:
    content = requests.get(sitemap_url).content
    soup = BeautifulSoup(content, "xml")

    filtered_urls = []
    for sitemap in soup.find_all("sitemap"):
        lastmod_element = sitemap.find("lastmod")
        lastmod_date = lastmod_element.text

        if lastmod_date > date_threshold:
            loc_element = sitemap.find("loc")
            filtered_urls.append(loc_element.text)

    async with aiohttp.ClientSession() as session:
        # urls = await fetch_sitemap_urls(sitemap_url, session)
        tasks = [fetch_sitemap_urls(url, session) for url in filtered_urls]

        ret = await asyncio.gather(*tasks)
        ret = [x for x in ret if x is not None]
        ret = [item for sublist in ret for item in sublist]
        ret = [x for x in ret if "/recipe/" in x]

    return ret
