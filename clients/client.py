import httpx

def get_client():
    return httpx.AsyncClient(timeout=10.0)
