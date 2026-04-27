import logging
import asyncio
import httpx

logger = logging.getLogger("post_call")

# Don't retry on these — they won't change on retry
NON_RETRYABLE_CODES = {400, 401, 403, 404, 422}

async def safe_request(client, method, url, **kwargs):
    last_exc = None

    for attempt in range(3):
        try:
            res = await client.request(method, url, **kwargs)

            # Log full response for debugging
            logger.info(f"[{method} {url}] status={res.status_code}")
            if res.status_code >= 400:
                logger.error(
                    f"[{method} {url}] error body: {res.text}"
                )

            # Don't retry client errors — they won't resolve
            if res.status_code in NON_RETRYABLE_CODES:
                res.raise_for_status()

            res.raise_for_status()
            return res

        except httpx.HTTPStatusError as e:
            logger.error(f"Request failed (attempt {attempt + 1}): {e}")
            last_exc = e

            if e.response.status_code in NON_RETRYABLE_CODES:
                raise  # fail immediately, no retry

            if attempt < 2:
                await asyncio.sleep(1 * (attempt + 1))

        except Exception as e:
            logger.error(f"Request failed (attempt {attempt + 1}): {e}")
            last_exc = e
            if attempt < 2:
                await asyncio.sleep(1 * (attempt + 1))

    raise last_exc