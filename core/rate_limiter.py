import time
from fastapi import Request, HTTPException

RATE_LIMIT = {}
MAX_REQUESTS = 20
WINDOW = 60  

def rate_limiter(request: Request):
    ip = request.client.host
    now = time.time()

    if ip not in RATE_LIMIT:
        RATE_LIMIT[ip] = []

    RATE_LIMIT[ip] = [t for t in RATE_LIMIT[ip] if now - t < WINDOW]

    if len(RATE_LIMIT[ip]) >= MAX_REQUESTS:
        raise HTTPException(status_code=429, detail="Too many requests")

    RATE_LIMIT[ip].append(now)