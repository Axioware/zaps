import logging
import time
from fastapi import FastAPI, HTTPException, Header, Depends, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

# Import custom modules
from config.database import init_db, get_row_limit, update_row_limit
from config.config import ADMIN_SECRET_KEY
from api.fus_bot_new_lead import Router as LeadRouter
from api.fus_bot_call_end import Router as CallEndRouter
from api.fus_bot_post_call import Router as PostCallRouter

# ------------------- LOGGING -------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("app")

# ------------------- APP INIT -------------------
app = FastAPI(title="Lead Automation System")

# ------------------- STARTUP -------------------
@app.on_event("startup")
def startup_event():
    init_db()
    logger.info("Database initialized.")

# ------------------- MIDDLEWARE -------------------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    
    response = await call_next(request)
    
    duration = round(time.time() - start_time, 3)
    logger.info(f"{request.method} {request.url} - {duration}s")
    
    return response

# ------------------- BASIC RATE LIMIT -------------------
# (Simple in-memory limiter per IP)
RATE_LIMIT = {}
MAX_REQUESTS = 20
WINDOW = 60  # seconds

def rate_limiter(request: Request):
    ip = request.client.host
    now = time.time()

    if ip not in RATE_LIMIT:
        RATE_LIMIT[ip] = []

    # remove old requests
    RATE_LIMIT[ip] = [t for t in RATE_LIMIT[ip] if now - t < WINDOW]

    if len(RATE_LIMIT[ip]) >= MAX_REQUESTS:
        raise HTTPException(status_code=429, detail="Too many requests")

    RATE_LIMIT[ip].append(now)

# ------------------- ADMIN SECURITY -------------------
def verify_admin(x_api_key: str = Header(...)):
    if not x_api_key or x_api_key != ADMIN_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")

# ------------------- SCHEMA -------------------
class ConfigUpdate(BaseModel):
    num_rows: int

# ------------------- ROUTERS -------------------
app.include_router(LeadRouter, prefix="/api/leads", tags=["Lead Processing"])
app.include_router(CallEndRouter, prefix="/api/callback", tags=["Call Analysis"])
app.include_router(PostCallRouter, prefix="/api/postcall", tags=["Post-Call Logging"])

# ------------------- CONFIG ENDPOINTS -------------------
@app.get("/config", dependencies=[Depends(rate_limiter)])
async def view_config(_: str = Depends(verify_admin)):
    return {"num_rows": get_row_limit()}

@app.post("/config", dependencies=[Depends(rate_limiter)])
async def update_config(data: ConfigUpdate, _: str = Depends(verify_admin)):
    update_row_limit(data.num_rows)
    return {"message": f"Limit updated to {data.num_rows}"}

# ------------------- SIMPLE UI -------------------
@app.get("/", response_class=HTMLResponse)
async def simple_ui():
    current_limit = get_row_limit()
    
    return f"""
    <html>
        <head>
            <title>Lead Bot Control Panel</title>
        </head>
        <body style="font-family:sans-serif;text-align:center;margin-top:50px;">
            <h2>Lead Bot Settings</h2>
            <p>Current Batch Size: <b>{current_limit}</b></p>

            <input type="password" id="pw" placeholder="Admin Key"><br><br>
            <input type="number" id="rows" placeholder="New Row Limit"><br><br>

            <button onclick="save()">Update</button>

            <script>
                async function save(){{
                    const key = document.getElementById('pw').value;
                    const val = document.getElementById('rows').value;

                    const res = await fetch('/config', {{
                        method: 'POST',
                        headers: {{
                            'x-api-key': key,
                            'Content-Type': 'application/json'
                        }},
                        body: JSON.stringify({{num_rows: val}})
                    }});

                    const out = await res.json();

                    if(res.ok){{
                        alert(out.message);
                        location.reload();
                    }} else {{
                        alert(out.detail || "Error");
                    }}
                }}
            </script>
        </body>
    </html>
    """