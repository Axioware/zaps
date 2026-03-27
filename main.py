import logging, time, asyncio

from fastapi import FastAPI, Header, Depends, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from core.rate_limiter import rate_limiter
from config.database import init_db, get_row_limit, update_row_limit
from config.config import ADMIN_SECRET_KEY
from api.fus_bot_new_lead import Router as LeadRouter
from api.fus_bot_call_end import Router as CallEndRouter
from api.fus_bot_post_call import Router as PostCallRouter
from api.alab_sheets_bot import Router as AlabSheetsRouter
from core.security import verify_admin
from api.sheets import router as SheetsRouter
from services.scheduler_service import start_scheduler, scheduler_loop






logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("app")

app = FastAPI(title="Lead Automation System")

@app.on_event("startup")
def startup_event():
    init_db()
    start_scheduler()
    logger.info("Database initialized.")

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    
    response = await call_next(request)
    
    duration = round(time.time() - start_time, 3)
    logger.info(f"{request.method} {request.url} - {duration}s")
    
    return response


class ConfigUpdate(BaseModel):
    num_rows: int

app.include_router(LeadRouter, prefix="/api/leads", tags=["Lead Processing"])
app.include_router(CallEndRouter, prefix="/api/callback", tags=["Call Analysis"])
app.include_router(PostCallRouter, prefix="/api/postcall", tags=["Post-Call Logging"])
app.include_router(AlabSheetsRouter,prefix="/api/alab-sheets",tags=["ALab Sheets Bot"])
app.include_router(SheetsRouter, prefix="/api", tags=["Sheets"])

@app.get("/config", dependencies=[Depends(rate_limiter)])
async def view_config(_: str = Depends(verify_admin)):
    return {"num_rows": get_row_limit()}

@app.post("/config", dependencies=[Depends(rate_limiter)])
async def update_config(data: ConfigUpdate, _: str = Depends(verify_admin)):
    update_row_limit(data.num_rows)
    return {"message": f"Limit updated to {data.num_rows}"}

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