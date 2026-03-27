import logging
from fastapi import APIRouter, BackgroundTasks
from services.workflow_service import run_outbound_workflow

Router = APIRouter()
logger = logging.getLogger("lead_workflow")

@Router.post("/trigger")
async def trigger_webhook(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_outbound_workflow)
    return {"status": "Workflow started"}


