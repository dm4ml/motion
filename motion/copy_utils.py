"""
This file has utilities to copy components and their state
from one Redis instance to another.
"""

import logging

import redis.asyncio as redis

from motion.utils import RedisParams

logger = logging.getLogger(__name__)


async def copy_db(src: RedisParams, dest: RedisParams) -> None:
    """
    Copy a component and its state from one Redis instance to another.

    Args:
        src: RedisParams for the source Redis instance.
        dest: RedisParams for the destination Redis instance.
    """

    # Verify that src and dest are different
    if src.dict() is dest.dict():
        raise ValueError("Source and destination must be different.")

    # establish connections

    src_con: redis.Redis = redis.Redis(**src.dict())

    if await src_con.ping() is False:
        raise ValueError("Could not connect to source Redis instance.")

    dest_con: redis.Redis = redis.Redis(**dest.dict())

    if await dest_con.ping() is False:
        raise ValueError("Could not connect to destination Redis instance.")

    # Copy all keys prefixed MOTION_STATE: and MOTION_VERSION:
    try:
        key_prefixes = ["MOTION_STATE:", "MOTION_VERSION:"]
        for key_prefix in key_prefixes:
            logger.info(f"Copying keys with prefix {key_prefix}")

            cursor = 0
            while True:
                cursor, keys = await src_con.scan(
                    cursor=cursor, match=f"{key_prefix}*", count=1000
                )
                if not keys:
                    break

                # Pipeline to fetch all values in a single round trip
                pipeline = src_con.pipeline()
                for key in keys:
                    pipeline.get(key)
                values = await pipeline.execute()

                # Pipeline to set all values in the destination Redis
                pipeline = dest_con.pipeline()
                for key, value in zip(keys, values):
                    pipeline.set(key, value)
                await pipeline.execute()

                logger.info(f"Copied {len(keys)} keys with prefix {key_prefix}")

                # Make sure to convert cursor back to an integer if it's not already
                cursor = int(cursor) if cursor is not None else None
                if cursor == 0:
                    break

            logger.info(f"Finished copying keys with prefix {key_prefix}.")

    except Exception as e:
        logger.error(
            "Error copying Motion database. Please try again. "
            + f"Partial results may have completed. {e}",
            exc_info=True,
        )
        raise e

    finally:
        logger.info("Finished copying Motion database.")
        await src_con.close()
        await dest_con.close()
