import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from config.database import get_connection

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/prompts", tags=["Prompts"])


class PromptCreate(BaseModel):
    prompt_text: str
    active: bool = True


class PromptUpdate(BaseModel):
    prompt_text: str | None = None
    active: bool | None = None


class PromptResponse(BaseModel):
    id: int
    prompt_text: str
    active: bool
    created_at: str
    updated_at: str


@router.get("", response_model=list[PromptResponse])
async def get_prompts():
    """Get all prompts."""
    try:
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT id, prompt_text, active, created_at, updated_at FROM prompts ORDER BY id DESC"
            ).fetchall()
            
            return [
                PromptResponse(
                    id=row["id"],
                    prompt_text=row["prompt_text"],
                    active=row["active"],
                    created_at=str(row["created_at"]),
                    updated_at=str(row["updated_at"]),
                )
                for row in rows
            ]
    except Exception as e:
        logger.error(f"Error fetching prompts: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch prompts")


@router.get("/active", response_model=PromptResponse)
async def get_active_prompt():
    """Get the first active prompt."""
    try:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT id, prompt_text, active, created_at, updated_at FROM prompts WHERE active=TRUE ORDER BY id LIMIT 1"
            ).fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="No active prompt found")
            
            return PromptResponse(
                id=row["id"],
                prompt_text=row["prompt_text"],
                active=row["active"],
                created_at=str(row["created_at"]),
                updated_at=str(row["updated_at"]),
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching active prompt: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch active prompt")


@router.post("", response_model=PromptResponse)
async def create_prompt(data: PromptCreate):
    """Create a new prompt."""
    try:
        with get_connection() as conn:
            row = conn.execute(
                "INSERT INTO prompts (prompt_text, active) VALUES (%s, %s) RETURNING id, prompt_text, active, created_at, updated_at",
                (data.prompt_text, data.active)
            ).fetchone()
            
            conn.commit()
            
            return PromptResponse(
                id=row["id"],
                prompt_text=row["prompt_text"],
                active=row["active"],
                created_at=str(row["created_at"]),
                updated_at=str(row["updated_at"]),
            )
    except Exception as e:
        logger.error(f"Error creating prompt: {e}")
        raise HTTPException(status_code=500, detail="Failed to create prompt")


@router.put("/{prompt_id}", response_model=PromptResponse)
async def update_prompt(prompt_id: int, data: PromptUpdate):
    """Update a prompt by ID."""
    try:
        with get_connection() as conn:
            # Check if prompt exists
            existing = conn.execute(
                "SELECT id FROM prompts WHERE id=%s",
                (prompt_id,)
            ).fetchone()
            
            if not existing:
                raise HTTPException(status_code=404, detail="Prompt not found")
            
            # Build update query
            updates = []
            params = []
            
            if data.prompt_text is not None:
                updates.append("prompt_text=%s")
                params.append(data.prompt_text)
            
            if data.active is not None:
                updates.append("active=%s")
                params.append(data.active)
            
            if not updates:
                raise HTTPException(status_code=400, detail="No fields to update")
            
            updates.append("updated_at=CURRENT_TIMESTAMP")
            params.append(prompt_id)
            
            query = f"UPDATE prompts SET {', '.join(updates)} WHERE id=%s RETURNING id, prompt_text, active, created_at, updated_at"
            
            row = conn.execute(query, params).fetchone()
            conn.commit()
            
            return PromptResponse(
                id=row["id"],
                prompt_text=row["prompt_text"],
                active=row["active"],
                created_at=str(row["created_at"]),
                updated_at=str(row["updated_at"]),
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating prompt: {e}")
        raise HTTPException(status_code=500, detail="Failed to update prompt")
