import logging
import asyncio

logger = logging.getLogger("post_call")

async def safe_request(client, method, url, **kwargs):
    for attempt in range(3):
        try:
            res = await client.request(method, url, **kwargs)
            print(res)
            res.raise_for_status()
            return res
        except Exception as e:
            logger.error(f"Request failed (attempt {attempt + 1}): {str(e)}")
            if attempt == 2:
                raise
            await asyncio.sleep(1 * (attempt + 1))

