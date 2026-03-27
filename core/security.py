from fastapi import Header, HTTPException
from config.config import ADMIN_SECRET_KEY

def verify_webhook(x_api_key: str = Header(None)):
    if not x_api_key or x_api_key != ADMIN_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized webhook")

def verify_admin(x_api_key: str = Header(...)):
    if not x_api_key or x_api_key != ADMIN_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")