import logging
from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import HTMLResponse

# Import our custom modules
from config.database import init_db, get_row_limit, update_row_limit
from config.config import ADMIN_SECRET_KEY
from api.fus_bot_new_lead import Router as LeadRouter
from api.fus_bot_call_end import Router as CallEndRouter
from api.fus_bot_post_call import Router as PostCallRouter

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

app = FastAPI(title="Lead Automation System")

# Initialize database on startup
@app.on_event("startup")
def startup_event():
    init_db()
    logging.info("SQLite Database initialized and connected.")

app.include_router(LeadRouter, prefix="/api/leads", tags=["Lead Processing"])
app.include_router(CallEndRouter, prefix="/api/callback", tags=["Call Analysis"])
app.include_router(PostCallRouter, prefix="/api/callback", tags=["Post-Call Logging"])

# --- CONFIGURATION ENDPOINTS ---

@app.get("/config")
async def view_config(x_api_key: str = Header(None)):
    if x_api_key != ADMIN_SECRET_KEY: 
        raise HTTPException(status_code=403, detail="Unauthorized")
    return {"num_rows": get_row_limit()}

@app.post("/config")
async def update_config(data: dict, x_api_key: str = Header(None)):
    if x_api_key != ADMIN_SECRET_KEY: 
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    try:
        new_val = int(data.get("num_rows", 5))
        update_row_limit(new_val)
        return {"message": f"Limit successfully updated to {new_val}"}
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid number format")

# --- SIMPLE CONTROL PANEL UI ---

@app.get("/", response_class=HTMLResponse)
async def simple_ui():
    # Fetch current limit to display it on the page
    current_limit = get_row_limit()
    
    return f"""
    <html>
        <head>
            <title>Lead Bot Control Panel</title>
            <style>
                body {{ font-family: sans-serif; text-align: center; padding-top: 50px; background-color: #f8f9fa; }}
                .card {{ 
                    display: inline-block; background: white; padding: 40px; 
                    border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); 
                    min-width: 300px;
                }}
                input {{ padding: 12px; margin: 8px; width: 80%; border: 1px solid #ddd; border-radius: 4px; }}
                button {{ 
                    padding: 12px 24px; background: #007bff; color: white; 
                    border: none; border-radius: 6px; cursor: pointer; font-weight: bold;
                }}
                button:hover {{ background: #0056b3; }}
                hr {{ margin: 20px 0; border: 0; border-top: 1px solid #eee; }}
            </style>
        </head>
        <body>
            <div class="card">
                <h1>Lead Bot Settings</h1>
                <p>Current Batch Size: <strong>{current_limit} leads</strong></p>
                <hr>
                <input type="password" id="pw" placeholder="Admin Secret Key"><br>
                <input type="number" id="rows" placeholder="Set New Row Limit"><br><br>
                <button onclick="save()">Update Settings</button>
            </div>

            <script>
                async function save(){{
                    const key = document.getElementById('pw').value;
                    const val = document.getElementById('rows').value;
                    
                    if(!key || !val) {{
                        alert("Please provide both the Admin Key and a Row Limit.");
                        return;
                    }}

                    const res = await fetch('/config', {{
                        method: 'POST',
                        headers: {{'x-api-key': key, 'Content-Type': 'application/json'}},
                        body: JSON.stringify({{num_rows: val}})
                    }});
                    
                    const out = await res.json();
                    if(res.ok) {{
                        alert(out.message);
                        location.reload(); // Refresh to show the updated value
                    }} else {{
                        alert("Error: " + (out.detail || "Unauthorized"));
                    }}
                }}
            </script>
        </body>
    </html>
    """