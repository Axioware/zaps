from __future__ import annotations
import json
import logging
import os
import re
import tempfile
from datetime import datetime, timezone
from typing import Any
import anthropic
import gspread
import httpx
import openai
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from google.oauth2.service_account import Credentials
from clients.client import get_client
from config.config import SF_INSTANCE_URL
from services.salesforce_service import get_sf_access_token
from utils.retry import safe_request

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/smrt", tags=["smrt"])

# ---------------------------------------------------------------------------
# Hardcoded sheet config
# ---------------------------------------------------------------------------
_SMRT_SHEET_URL       = "https://docs.google.com/spreadsheets/d/1bk-G0lD3P9J6MSBYmMYLHfA-_aQ1FO-BTe0x20V6_Ok/edit"
_SMRT_SHEET_ID        = "1bk-G0lD3P9J6MSBYmMYLHfA-_aQ1FO-BTe0x20V6_Ok"
_SMRT_WORKSHEET_NAME  = "Sellers First AGENTS"

# ---------------------------------------------------------------------------
# In-memory accumulator (per call_id).
# Accumulates payloads until we have everything we need.
# ---------------------------------------------------------------------------
_call_store: dict[str, dict[str, Any]] = {}


def _get_or_create(call_id: str) -> dict[str, Any]:
    if call_id not in _call_store:
        _call_store[call_id] = {
            "call_id": call_id,
            "completed": False,
            "transcript_text": None,
            "audio_url": None,
            "summary_text": None,
            "keywords": [],
            "caller": None,
            "receiver": None,
            "timestamp": None,
            "status": None,
            "processed": False,
        }
    return _call_store[call_id]


# ---------------------------------------------------------------------------
# Webhook endpoint
# ---------------------------------------------------------------------------

@router.post("/call-ended")
async def smrt_call_ended(request: Request, background_tasks: BackgroundTasks):
    """
    Single endpoint that receives all SMRT Studio webhook events.
    The platform sends multiple payloads per call (one per event type).
    """
    try:
        payload: dict = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    event_type: str = payload.get("event") or payload.get("type") or ""
    call_id: str = (
        payload.get("callId")
        or payload.get("call_id")
        or payload.get("id")
        or "unknown"
    )

    logger.info("SMRT event=%s call_id=%s", event_type, call_id)

    record = _get_or_create(call_id)

    # ---- Normalise each event type ----------------------------------------

    if "status" in event_type.lower() or event_type == "call_status_updated":
        record["status"] = payload.get("status") or payload.get("callStatus")
        logger.info("Status update for %s → %s", call_id, record["status"])

    elif "complet" in event_type.lower() or event_type == "call_completed":
        record["completed"] = True
        record["timestamp"] = payload.get("timestamp") or payload.get("endedAt")
        record["caller"] = payload.get("caller") or payload.get("from")
        record["receiver"] = payload.get("receiver") or payload.get("to")
        # Some platforms embed audio URL in the completed event
        record["audio_url"] = (
            record["audio_url"]
            or payload.get("recordingUrl")
            or payload.get("audioUrl")
        )

    elif "transcript" in event_type.lower():
        # AI transcript event — plain text preferred
        record["transcript_text"] = (
            payload.get("transcript")
            or payload.get("transcriptText")
            or payload.get("text")
        )
        record["audio_url"] = (
            record["audio_url"]
            or payload.get("audioUrl")
            or payload.get("recordingUrl")
        )
        record["caller"] = record["caller"] or payload.get("caller")
        record["receiver"] = record["receiver"] or payload.get("receiver")
        record["timestamp"] = record["timestamp"] or payload.get("timestamp")

    elif "summary" in event_type.lower():
        record["summary_text"] = payload.get("summary") or payload.get("text")

    elif "keyword" in event_type.lower():
        record["keywords"] = payload.get("keywords") or []

    # ---- Decide whether to run the full pipeline --------------------------

    ready = (
        not record["processed"]
        and record["completed"]
        and (record["transcript_text"] or record["audio_url"])
    )

    if ready:
        record["processed"] = True  # prevent double-processing
        background_tasks.add_task(_run_pipeline, dict(record))

    return {"status": "ok", "call_id": call_id, "pipeline_triggered": ready}


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

async def _run_pipeline(record: dict):
    call_id = record["call_id"]
    logger.info("Pipeline starting for call_id=%s", call_id)

    try:
        # 1. Get transcript text
        transcript = await _get_transcript(record)
        if not transcript:
            logger.warning("No transcript for call_id=%s — aborting", call_id)
            return

        # 2. Score with Claude
        analysis = await _score_with_claude(transcript)

        # 3. Salesforce Chatter (async — uses bearer token like existing code)
        sf_lead_id = await _resolve_salesforce_lead(record)
        if sf_lead_id:
            await _post_to_salesforce_chatter(sf_lead_id, analysis)
        else:
            logger.warning("No Salesforce lead found for call_id=%s", call_id)

        # 4. Google Sheets
        _append_to_google_sheet(record, analysis, transcript)

        logger.info("Pipeline complete for call_id=%s", call_id)

    except Exception as exc:
        logger.exception("Pipeline failed for call_id=%s: %s", call_id, exc)


# ---------------------------------------------------------------------------
# Step 1 - Transcript
# ---------------------------------------------------------------------------

async def _get_transcript(record: dict) -> str | None:
    if record.get("transcript_text"):
        return record["transcript_text"]

    audio_url = record.get("audio_url")
    if not audio_url:
        return None

    logger.info("Downloading audio for Whisper: %s", audio_url)
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.get(audio_url)
        resp.raise_for_status()

    with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
        tmp.write(resp.content)
        tmp_path = tmp.name

    try:
        openai_client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
        with open(tmp_path, "rb") as audio_file:
            result = openai_client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
                response_format="text",
            )
        return result
    finally:
        os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# Step 2 - Claude scoring
# ---------------------------------------------------------------------------

SCORING_SYSTEM_PROMPT = """You are a sales call quality analyst for a real estate wholesaling company.

Your job is to:
1) Identify the call type
2) Score the call using the correct rubric

Be STRICT. Do NOT be generous. Penalize surface-level conversations.

========================
STEP 1: CLASSIFY CALL
========================

Determine if this is:

"intro_call" OR "process_call"

Definitions:

INTRO CALL:
- Early stage
- Seller exploring / not fully committed
- High-level discovery
- Light qualification

PROCESS CALL:
- Seller is ready or close to ready
- Rep is qualifying for an offer
- Deep dive into condition, price, and timeline expected

Rules:
- If seller expresses desire to sell AND rep begins detailed qualification → process_call
- If conversation stays high-level → intro_call

========================
STEP 2: SCORING (1-10 SCALE)
========================

ALL SCORES MUST BE 1-10

----------------------------------------
IF call_type = "intro_call":
----------------------------------------

Score based on EARLY STAGE performance:

1. Opening / Rapport: 1-10  
2. Discovery (basic info gathering): 1-10  
3. Motivation Identification: 1-10  
4. Communication / Control: 1-10  
5. Next Step Set: 1-10  

Guidelines:
- DO NOT penalize for lack of deep condition or price discovery
- Focus on engagement, clarity, and direction

----------------------------------------
IF call_type = "process_call":
----------------------------------------

You are STRICTLY grading FULL QUALIFICATION.

1. Opening / professionalism: 1-10  
Did the rep introduce themselves clearly, sound confident, and build rapport?

---

2. Going Deep (CRITICAL): 1-10  
Did the rep go beyond surface-level answers?

This is one of the most important categories.

- If seller gives a reason (example: "downsizing"), did the rep ask:
  - Why are you downsizing?
  - What triggered that decision?
  - When did this start?
  - What happens if you don't sell?

- Did the rep ask MULTIPLE follow-up questions after the first answer?
- Did the rep peel back layers and uncover real motivation?

Scoring guidance:
1-3 = Surface-level, accepts first answer  
4-6 = Some follow-ups but not deep  
7-8 = Solid probing, some depth  
9-10 = Excellent layering, uncovering root cause  

---

3. Motivation: 1-10  
Did the rep clearly uncover WHY the seller wants to sell?

Look for:
distress, pain, timeline pressure, financial issues, life events

Scoring guidance:
1-3 = Weak or unclear motivation  
4-6 = Basic motivation identified  
7-8 = Strong motivation uncovered  
9-10 = Deep emotional or financial drivers uncovered  

---

4. Urgency / timeline: 1-10  
Did the rep identify WHEN seller wants to sell and how urgent it is?

Scoring guidance:
1-3 = No timeline  
4-6 = Vague timeline  
7-8 = Clear timeline  
9-10 = Urgency + consequences identified  

---

5. Property Condition (DEPTH + COVERAGE): 1-10  

The rep must BOTH:
(A) Ask about condition  
(B) Go deep and cover multiple categories  

Evaluate based on how many of these were covered AND how detailed:

Fields expected:
- Bedrooms / Bathrooms (and verification)
- Square footage (and verification)
- Lot size
- Solar
- Roof (age, type, condition)
- Foundation
- Windows (age, condition)
- Structural issues
- Siding
- Balcony / decks
- Kitchen (age, upgrades, condition)
- Water intrusion / mold
- Bathrooms (condition, upgrades)
- Sewer or septic
- Flooring
- Permits
- Garage (type, condition, electric, size)
- Pool
- Plumbing
- Electrical (panel, condition)
- HVAC / Furnace / AC
- Water heater
- Repairs needed
- Special features
- Anything else important

Scoring guidance:
1-3 = Bare minimum ("does it need repairs?")  
4-6 = Some categories, shallow  
7-8 = Good coverage, moderate depth  
9-10 = Thorough, detailed, multiple systems covered  

---

6. Price / equity / payoff: 1-10  

Did the rep ask:
- Asking price?
- Lowest they'd take?
- Mortgage/payoff?
- Equity?
- Net expectation?

Scoring guidance:
1-3 = No price discussion  
4-6 = Basic price asked  
7-8 = Good financial discovery  
9-10 = Full financial picture uncovered  

---

7. Objection handling: 1-10  
Did the rep respond well to hesitation or pushback?

Scoring guidance:
1-3 = Poor or no handling  
4-6 = Basic handling  
7-8 = Solid handling  
9-10 = Strong control and reframing  

---

8. Clear next step: 1-10  
Did the rep set a clear next action?

Scoring guidance:
1-3 = No next step  
4-6 = Weak or vague next step  
7-8 = Clear next step  
9-10 = Strong, controlled next step with commitment  

========================
OUTPUT FORMAT (STRICT JSON)
========================

Return ONLY valid JSON. No preamble. No markdown fences.

{
  "call_type": "",
  "overall_score": 0,
  "grade": "",

  "opening_score": 0,
  "going_deep_score": 0,
  "motivation_score": 0,
  "urgency_score": 0,
  "condition_score": 0,
  "price_score": 0,
  "objection_score": 0,
  "next_step_score": 0,

  "call_summary": "",
  "seller_motivation": "",
  "seller_urgency": "",
  "property_condition": "",
  "price_notes": "",
  "missed_questions": [],
  "rep_feedback": "",
  "next_best_action": "",
  "coaching_summary_for_slack": ""
}

========================
GRADING SCALE
========================

PROCESS CALL (total out of 80):
72-80 = A  
64-71 = B  
56-63 = C  
48-55 = D  
Below 48 = F  

INTRO CALL:
Use general judgment based on 1-10 averages

========================
IMPORTANT RULES
========================

- Be critical. Do NOT inflate scores.
- If call_type = process_call AND Going Deep <= 5 → cap overall_score <= 7.5
- If no clear motivation → cap overall_score <= 6.5
- Reward reps who CONTROL the conversation and DIG
"""


async def _score_with_claude(transcript: str) -> dict:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2048,
        system=SCORING_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"Please score this call transcript:\n\n{transcript}",
            }
        ],
    )

    raw = message.content[0].text.strip()
    # Strip markdown fences if present
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("Claude returned non-JSON: %s", raw[:500])
        raise ValueError(f"Claude JSON parse error: {exc}") from exc


# ---------------------------------------------------------------------------
# Step 3 - Salesforce (httpx + bearer token — same pattern as sf_sheets_bot.py)
# ---------------------------------------------------------------------------

async def _resolve_salesforce_lead(record: dict) -> str | None:
    """
    Find a Salesforce Lead by the caller's phone number.
    Uses the same bearer-token SOQL pattern as the rest of the project.
    """
    phone = record.get("caller") or record.get("receiver")
    if not phone:
        return None

    digits  = re.sub(r"\D", "", str(phone))
    last_10 = digits[-10:] if len(digits) >= 10 else digits
    if not last_10:
        return None

    access_token = await get_sf_access_token()
    sf_headers   = {"Authorization": f"Bearer {access_token}"}
    query_url    = f"{SF_INSTANCE_URL}/services/data/v57.0/query"

    async with get_client() as client:
        res = await safe_request(
            client, "GET", query_url,
            params={"q": f"SELECT Id FROM Lead WHERE Phone LIKE '%{last_10}%' LIMIT 1"},
            headers=sf_headers,
        )
        records = res.json().get("records", [])
        if records:
            return records[0]["Id"]

        # Fallback: Contact
        res = await safe_request(
            client, "GET", query_url,
            params={"q": f"SELECT Id FROM Contact WHERE Phone LIKE '%{last_10}%' LIMIT 1"},
            headers=sf_headers,
        )
        records = res.json().get("records", [])
        return records[0]["Id"] if records else None


def _build_chatter_body(analysis: dict) -> str:
    """
    Three-section Chatter post:
      1. Call Summary
      2. Scores
      3. Feedback & Recommendations + Next Best Action
    """
    a         = analysis
    call_type = a.get("call_type", "unknown").replace("_", " ").title()
    overall   = a.get("overall_score", "N/A")
    grade     = a.get("grade", "N/A")

    lines = [
        f"📞 CALL QUALITY ANALYSIS — {call_type}",
        f"Overall Score: {overall}/10  |  Grade: {grade}",
        "",
        "─── CALL SUMMARY ───",
        a.get("call_summary", ""),
        "",
        "─── SCORES ───",
        f"Opening / Professionalism : {a.get('opening_score', 'N/A')}",
        f"Going Deep               : {a.get('going_deep_score', 'N/A')}",
        f"Motivation               : {a.get('motivation_score', 'N/A')}",
        f"Urgency / Timeline       : {a.get('urgency_score', 'N/A')}",
        f"Property Condition       : {a.get('condition_score', 'N/A')}",
        f"Price / Equity / Payoff  : {a.get('price_score', 'N/A')}",
        f"Objection Handling       : {a.get('objection_score', 'N/A')}",
        f"Clear Next Step          : {a.get('next_step_score', 'N/A')}",
        "",
        "─── FEEDBACK & RECOMMENDATIONS ───",
        a.get("rep_feedback", ""),
        "",
        "─── NEXT BEST ACTION ───",
        a.get("next_best_action", ""),
    ]

    missed = a.get("missed_questions", [])
    if missed:
        lines += ["", "─── MISSED QUESTIONS ───"]
        lines += [f"• {q}" for q in missed]

    return "\n".join(lines)


async def _post_to_salesforce_chatter(lead_id: str, analysis: dict):
    """
    Post a new Chatter FeedItem on the Lead record.
    Uses httpx + bearer token — same auth pattern as the rest of the project.
    Each call creates a NEW post; nothing is overwritten.
    """
    access_token = await get_sf_access_token()
    chatter_url  = f"{SF_INSTANCE_URL}/services/data/v57.0/chatter/feed-elements"
    body_text    = _build_chatter_body(analysis)

    payload = {
        "body": {
            "messageSegments": [
                {"type": "Text", "text": body_text}
            ]
        },
        "feedElementType": "FeedItem",
        "subjectId": lead_id,
    }

    async with get_client() as client:
        res = await safe_request(
            client, "POST", chatter_url,
            json=payload,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type":  "application/json",
            },
        )

    logger.info("Chatter posted for lead %s | status=%s", lead_id, res.status_code)


# ---------------------------------------------------------------------------
# Step 4 - Google Sheets
# ---------------------------------------------------------------------------

def _get_sheets_client() -> gspread.Client:
    creds_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return gspread.authorize(creds)


# Column order — each score gets its own column as requested.
# Header row is written automatically on first run if missing.
_SHEET_HEADERS = [
    "Timestamp",
    "Call ID",
    "Caller",
    "Call Type",
    "Overall Score",
    "Grade",
    "Opening Score",
    "Going Deep Score",
    "Motivation Score",
    "Urgency Score",
    "Condition Score",
    "Price Score",
    "Objection Score",
    "Next Step Score",
    "Call Summary",
    "Seller Motivation",
    "Seller Urgency",
    "Property Condition",
    "Price Notes",
    "Rep Feedback",
    "Next Best Action",
    "Missed Questions",
    "Call Transcript",
]


def _ensure_header_row(worksheet: gspread.Worksheet) -> None:
    """Write the header row once if the sheet is brand new."""
    first_row = worksheet.row_values(1)
    if not first_row or first_row[0] != "Timestamp":
        worksheet.insert_row(_SHEET_HEADERS, index=1)
        logger.info("Header row written to '%s'", _SMRT_WORKSHEET_NAME)


def _append_to_google_sheet(record: dict, analysis: dict, transcript: str) -> None:
    try:
        gc = _get_sheets_client()
        sh = gc.open_by_key(_SMRT_SHEET_ID)

        try:
            ws = sh.worksheet(_SMRT_WORKSHEET_NAME)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=_SMRT_WORKSHEET_NAME, rows=1000, cols=30)
            logger.info("Created new worksheet '%s'", _SMRT_WORKSHEET_NAME)

    except Exception as exc:
        logger.error("Could not open Google Sheet: %s", exc)
        return

    _ensure_header_row(ws)

    a      = analysis
    missed = "; ".join(a.get("missed_questions", []))
    now    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    row = [
        now,
        record.get("call_id", ""),
        record.get("caller", ""),
        a.get("call_type", ""),
        a.get("overall_score", ""),
        a.get("grade", ""),
        a.get("opening_score", ""),
        a.get("going_deep_score", ""),
        a.get("motivation_score", ""),
        a.get("urgency_score", ""),
        a.get("condition_score", ""),
        a.get("price_score", ""),
        a.get("objection_score", ""),
        a.get("next_step_score", ""),
        a.get("call_summary", ""),
        a.get("seller_motivation", ""),
        a.get("seller_urgency", ""),
        a.get("property_condition", ""),
        a.get("price_notes", ""),
        a.get("rep_feedback", ""),
        a.get("next_best_action", ""),
        missed,
        transcript,
    ]

    ws.append_row(row, value_input_option="USER_ENTERED")
    logger.info(
        "Sheet row appended | call_id=%s | sheet=%s | tab=%s",
        record.get("call_id"), _SMRT_SHEET_ID, _SMRT_WORKSHEET_NAME,
    )