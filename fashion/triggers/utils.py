"""Helper functions to download data.
"""
import aiohttp
import asyncio
import logging


async def get(url, session):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"
    }
    try:
        async with session.get(url=url, headers=headers) as response:
            resp = await response.content.read()
            return url, resp
    except Exception as e:
        logging.error(
            "Unable to get url {} due to {}.".format(url, e.__class__)
        )


async def async_download_image(img_urls):
    async with aiohttp.ClientSession() as session:
        ret = await asyncio.gather(*[get(url, session) for url in img_urls])
        ret = [x for x in ret if x is not None]
        return [x[0] for x in ret], [x[1] for x in ret]
