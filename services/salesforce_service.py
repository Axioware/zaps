import logging
from config.config import SF_CLIENT_ID, SF_CLIENT_SECRET, SF_REFRESH_TOKEN
from clients.client import get_client
from utils.retry import safe_request

logger = logging.getLogger("post_call")

async def get_sf_access_token():
    url = "https://login.salesforce.com/services/oauth2/token"
    logger.info("Requesting Salesforce access token...")
    async with get_client() as client:
        res = await safe_request(
            client,
            "POST",
            url,
            data={
                "grant_type": "refresh_token",
                "client_id": SF_CLIENT_ID,
                "client_secret": SF_CLIENT_SECRET,
                "refresh_token": SF_REFRESH_TOKEN
            }
        )
        logger.info("Salesforce token received.")
        data = res.json()
        token = data.get("access_token")
        logger.info(f"Salesforce token: {'***' + token[-4:] if token else 'None'}")
        if not token:
            raise RuntimeError("Salesforce token missing")

        return token
