from __future__ import annotations

import logging
from typing import Any

import anthropic
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from config.config import ANTHROPIC_API_KEY
from config.database import (
    add_conversation_message,
    clear_conversation_messages,
    get_all_conversation_messages,
    get_recent_conversation_messages,
    get_connection,
    CONVERSATION_HISTORY_LIMIT,
)
from core.security import verify_admin

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Conversation"])


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class MessageRequest(BaseModel):
    message: str


class MessageResponse(BaseModel):
    user_message_id: int
    assistant_message_id: int
    reply: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_active_system_prompt() -> str:
    """Load the active system prompt from the DB (same as smrt_webhook uses)."""
    try:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT prompt_text FROM prompts WHERE active=TRUE ORDER BY id LIMIT 1"
            ).fetchone()
            if row and row["prompt_text"]:
                return row["prompt_text"]
    except Exception as exc:
        logger.warning("Failed to load active prompt from DB: %s", exc)

    return "insert prompt here"


def _normalise_messages(messages: list[dict]) -> list[dict]:
    """
    Merge consecutive same-role messages so the list always alternates
    user / assistant as required by the Anthropic API.
    """
    if not messages:
        return []

    normalised: list[dict] = []
    for msg in messages:
        if normalised and normalised[-1]["role"] == msg["role"]:
            normalised[-1]["content"] += "\n\n" + msg["content"]
        else:
            normalised.append({"role": msg["role"], "content": msg["content"]})

    return normalised


def _call_claude(user_message: str) -> str:
    """
    Send user_message to Claude with the full recent conversation history
    as context, then return Claude's plain-text reply.
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    system_prompt = _load_active_system_prompt()

    history = get_recent_conversation_messages()  # last CONVERSATION_HISTORY_LIMIT messages

    messages: list[dict] = []
    for row in history:
        role = "user" if row["message_from"] in ("user", "system") else "assistant"
        messages.append({"role": role, "content": row["message"]})

    messages.append({"role": "user", "content": user_message})
    messages = _normalise_messages(messages)

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system=system_prompt,
        messages=messages,
    )

    return (response.content[0].text or "").strip()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/conversation")
async def get_conversation() -> list[dict[str, Any]]:
    """
    Return every message in conversation history ordered oldest → newest.

    Response shape:
        [
            { "id": 1, "message_from": "user", "message": "...", "created_at": "..." },
            ...
        ]
    """
    rows = get_all_conversation_messages()
    # Convert datetime objects to ISO strings so they serialise cleanly
    return [
        {
            "id": r["id"],
            "message_from": r["message_from"],
            "message": r["message"],
            "created_at": str(r["created_at"]),
        }
        for r in rows
    ]


@router.post("/conversation/message", response_model=MessageResponse)
async def post_conversation_message(body: MessageRequest) -> MessageResponse:
    """
    Blake sends a message.

    1. Persist it as message_from="user".
    2. Send to Claude (with full history as context).
    3. Persist Claude's reply as message_from="assistant".
    4. Return both IDs and the reply text.
    """
    if not body.message.strip():
        raise HTTPException(status_code=400, detail="message must not be empty")

    # Save Blake's message first so it is part of history when Claude replies
    user_msg_id = add_conversation_message("user", body.message)

    try:
        reply = _call_claude(body.message)
    except Exception as exc:
        logger.error("Claude API error in conversation endpoint: %s", exc)
        raise HTTPException(status_code=502, detail=f"Claude API error: {exc}") from exc

    assistant_msg_id = add_conversation_message("assistant", reply)

    return MessageResponse(
        user_message_id=user_msg_id,
        assistant_message_id=assistant_msg_id,
        reply=reply,
    )


@router.delete("/conversation", dependencies=[Depends(verify_admin)])
async def delete_conversation() -> dict[str, Any]:
    """
    Clear the entire conversation history.
    Requires the X-Admin-Passcode header to equal the configured passcode.

    Response:
        { "deleted": <number of rows removed> }
    """
    deleted = clear_conversation_messages()
    return {"deleted": deleted}