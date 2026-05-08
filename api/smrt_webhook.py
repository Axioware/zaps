from __future__ import annotations
import io
import json
import logging
import os
import re
import resend
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo
import anthropic
import gspread
import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from faster_whisper import WhisperModel
from google.oauth2.service_account import Credentials
from clients.client import get_client
from config.config import SF_INSTANCE_URL
from config.database import get_connection
from services.salesforce_service import get_sf_access_token
from utils.retry import safe_request
from config.config import ANTHROPIC_API_KEY

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/smrt", tags=["smrt"])

_EMAIL_RECIPIENTS = ["connorg@sellersfirstre.com", "blakef@sellersfirstre.com"]
_EMAIL_SUBJECT = "call rubrics"

_SMRT_SHEET_ID        = "1bk-G0lD3P9J6MSBYmMYLHfA-_aQ1FO-BTe0x20V6_Ok"
_SMRT_WORKSHEET_NAME  = "Scoring Rubrics"

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
            "call_from": None,
            "call_to": None,
            "timestamp": None,
            "status": None,
            "caller_id_name": None,
            "user_name": None,
            "contact_name": None,
            "call_notes": None,
            "call_outcome": None,
            "smrt_phone_call_id": None,
            "device": None,
            "event": None,
            "processed": False,
            # new fields
            "record_id": None,
            "call_link": None,
            "lead_owner": None,
            "opportunity_owner": None,
            "duration": None,
        }
    return _call_store[call_id]


_WHISPER_MODEL: WhisperModel | None = None
_WHISPER_MODEL_SIZE = os.environ.get("WHISPER_MODEL", "small.en")
_WHISPER_DEVICE = os.environ.get("WHISPER_DEVICE", "cpu")


def _get_whisper_model() -> WhisperModel:
    global _WHISPER_MODEL
    if _WHISPER_MODEL is None:
        _WHISPER_MODEL = WhisperModel(_WHISPER_MODEL_SIZE, device=_WHISPER_DEVICE)
        logger.info("Loaded Whisper model %s on %s", _WHISPER_MODEL_SIZE, _WHISPER_DEVICE)
    return _WHISPER_MODEL


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

    record["event"] = event_type
    record["caller_id_name"] = record["caller_id_name"] or payload.get("callerIdName")
    record["user_name"] = record["user_name"] or payload.get("userName")
    record["contact_name"] = record["contact_name"] or payload.get("contactName")
    record["call_notes"] = record["call_notes"] or payload.get("callNotes")
    record["call_outcome"] = record["call_outcome"] or payload.get("callOutcome")
    record["smrt_phone_call_id"] = record["smrt_phone_call_id"] or payload.get("smrtPhoneCallId")
    record["device"] = record["device"] or payload.get("device")
    record["audio_url"] = record["audio_url"] or payload.get("recordingUrl") or payload.get("audioUrl")
    record["timestamp"] = record["timestamp"] or payload.get("date") or payload.get("timestamp") or payload.get("endedAt")
    # populate new fields from payload
    record["record_id"] = record["record_id"] or payload.get("recordId") or payload.get("record_id")
    record["call_link"] = record["call_link"] or payload.get("callLink") or payload.get("call_link")
    record["lead_owner"] = record["lead_owner"] or payload.get("leadOwner") or payload.get("lead_owner")
    record["opportunity_owner"] = record["opportunity_owner"] or payload.get("opportunityOwner") or payload.get("opportunity_owner")
    record["duration"] = record["duration"] or payload.get("duration") or payload.get("callDuration")

    if "status" in event_type.lower() or event_type == "call_status_updated":
        record["status"] = payload.get("status") or payload.get("callStatus")
        logger.info("Status update for %s → %s", call_id, record["status"])

    elif "complet" in event_type.lower() or event_type == "call_completed":
        record["completed"] = True
        # renamed: caller → call_from, receiver → call_to
        record["call_from"] = record["call_from"] or payload.get("caller") or payload.get("from")
        record["call_to"] = record["call_to"] or payload.get("receiver") or payload.get("to")
        logger.info("Marking completed for %s", call_id)

    elif "transcript" in event_type.lower():
        record["transcript_text"] = (
            payload.get("transcript")
            or payload.get("transcriptText")
            or payload.get("text")
        )
        record["call_from"] = record["call_from"] or payload.get("caller")
        record["call_to"] = record["call_to"] or payload.get("receiver")

    elif "summary" in event_type.lower():
        record["summary_text"] = payload.get("summary") or payload.get("text")

    elif "keyword" in event_type.lower():
        record["keywords"] = payload.get("keywords") or []

    ready = (
        not record["processed"]
        and record["completed"]
        and (record["transcript_text"] or record["audio_url"])
    )

    if ready:
        record["processed"] = True
        background_tasks.add_task(_run_pipeline, dict(record))

    return {"status": "ok", "call_id": call_id, "pipeline_triggered": ready}


async def _run_pipeline(record: dict):
    call_id = record["call_id"]
    logger.info("Pipeline starting for call_id=%s", call_id)

    try:
        transcript = await _get_transcript(record)
        if not transcript:
            logger.warning("No transcript for call_id=%s — aborting", call_id)
            return

        analysis = await _score_with_claude(transcript)

        # use call_from (previously caller) for SF lead resolution
        sf_lead_id = await _resolve_salesforce_lead(record)
        if sf_lead_id:
            await _post_to_salesforce_chatter(sf_lead_id, analysis, record)
        else:
            logger.warning("No Salesforce lead found for call_id=%s", call_id)

        await _send_email(analysis, record)

        _append_to_google_sheet(record, analysis, transcript)

        logger.info("Pipeline complete for call_id=%s", call_id)

    except Exception as exc:
        logger.exception("Pipeline failed for call_id=%s: %s", call_id, exc)


async def _get_transcript(record: dict) -> str | None:
    if record.get("transcript_text"):
        logger.info("Using pre-supplied transcript for call_id=%s", record["call_id"])
        return record["transcript_text"]

    audio_url = record.get("audio_url")
    if not audio_url:
        return None

    logger.info("Downloading audio for Whisper transcription: %s", audio_url)
    async with httpx.AsyncClient(timeout=120) as http:
        resp = await http.get(audio_url)
        resp.raise_for_status()
        audio_bytes = resp.content

    audio_io = io.BytesIO(audio_bytes)
    url_lower = audio_url.lower()
    extension = "mp3"
    if url_lower.endswith(".wav"):
        extension = "wav"
    elif url_lower.endswith(".ogg"):
        extension = "ogg"
    elif url_lower.endswith(".webm"):
        extension = "webm"
    audio_io.name = f"audio.{extension}"

    model = _get_whisper_model()
    segments, info = model.transcribe(
        audio_io,
        language="en",
        task="transcribe",
        without_timestamps=True,
    )

    if info.duration < 30:
        logger.warning(
            "Audio too short for call_id=%s | duration=%.1f seconds (minimum 30s required)",
            record["call_id"],
            info.duration,
        )
        return None

    transcript = " ".join(
        segment.text.strip() for segment in segments if segment.text.strip()
    )
    logger.info(
        "Whisper transcription complete for call_id=%s | length=%d | duration=%.1f seconds",
        record["call_id"],
        len(transcript),
        info.duration,
    )
    return transcript or None


def _extract_json_text(raw: str) -> str:
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned, flags=re.IGNORECASE)

    try:
        json.loads(cleaned)
        return cleaned
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end != -1 and end > start:
            candidate = cleaned[start:end + 1]
            try:
                json.loads(candidate)
                return candidate
            except json.JSONDecodeError:
                pass

    raise ValueError("Unable to parse valid JSON from Claude response")


def _load_active_system_prompt() -> str:
    try:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT prompt_text FROM prompts WHERE active=TRUE ORDER BY id LIMIT 1"
            ).fetchone()
            if row and row["prompt_text"]:
                return row["prompt_text"]
    except Exception as exc:
        logger.warning("Failed to load active prompt from DB: %s", exc)

    logger.warning("Using fallback default scoring prompt")
    return "insert prompt here"


async def _score_with_claude(transcript: str) -> dict:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    system_prompt = _load_active_system_prompt()

    prompt = (
        "Please score this call transcript and return ONLY valid JSON using the schema defined in the system prompt. "
        "Do not include any commentary, markdown fences, or extra text. "
        "If you cannot supply valid JSON, return an empty JSON object {}.\n\n"
        f"Transcript:\n\n{transcript}"
    )

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2048,
        system=system_prompt,
        messages=[
            {
                "role": "user",
                "content": prompt,
            }
        ],
    )

    raw = (message.content[0].text or "").strip()

    try:
        json_text = _extract_json_text(raw)
        return json.loads(json_text)
    except (ValueError, json.JSONDecodeError) as exc:
        logger.error("Claude returned invalid JSON for transcript scoring: %s", raw[:1000])
        raise ValueError(f"Claude JSON parse error: {exc}") from exc


async def _resolve_salesforce_lead(record: dict) -> str | None:
    # use call_from (previously caller) for phone lookup
    phone = record.get("call_from") or record.get("call_to")
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

        res = await safe_request(
            client, "GET", query_url,
            params={"q": f"SELECT Id FROM Contact WHERE Phone LIKE '%{last_10}%' LIMIT 1"},
            headers=sf_headers,
        )
        records = res.json().get("records", [])
        return records[0]["Id"] if records else None


def _build_chatter_body(analysis: dict, record: dict | None = None) -> str:
    a         = analysis
    call_type = a.get("call_type", "unknown").replace("_", " ").title()
    overall   = a.get("overall_score", "N/A")
    grade     = a.get("grade", "N/A")

    lines = [
        f"CALL QUALITY ANALYSIS — {call_type}",
        f"Overall Score: {overall}/10  |  Grade: {grade}",
    ]

    if record:
        user_name    = record.get("user_name", "N/A")
        contact_name = record.get("contact_name", "N/A")
        timestamp    = record.get("timestamp", "N/A")
        # updated to use call_from/call_to and include new fields
        call_from    = record.get("call_from", "N/A")
        call_to      = record.get("call_to", "N/A")
        lead_owner   = record.get("lead_owner", "N/A")
        opp_owner    = record.get("opportunity_owner", "N/A")
        duration     = record.get("duration", "N/A")
        lines.extend([
            "",
            "─── CALL DETAILS ───",
            f"Rep Name          : {user_name}",
            f"Contact Name      : {contact_name}",
            f"Date/Time         : {timestamp}",
            f"Call From         : {call_from}",
            f"Call To           : {call_to}",
            f"Lead Owner        : {lead_owner}",
            f"Opportunity Owner : {opp_owner}",
            f"Duration          : {duration}",
        ])

    lines.extend([
        "",
        "─── CALL SUMMARY ───",
        a.get("call_summary", ""),
        "",
        "─── SCORES ───",
        f"Opening / Professionalism : {a.get('opening_score', 'N/A')}",
        f"Rapport & Connection      : {a.get('rapport_connection_score', 'N/A')}",
        f"Going Deep                : {a.get('going_deep_score', 'N/A')}",
        f"Motivation                : {a.get('motivation_score', 'N/A')}",
        f"Urgency / Timeline        : {a.get('urgency_score', 'N/A')}",
        f"Property Condition        : {a.get('condition_score', 'N/A')}",
        f"Price / Equity / Payoff   : {a.get('price_score', 'N/A')}",
        f"Objection Handling        : {a.get('objection_score', 'N/A')}",
        f"Clear Next Step           : {a.get('next_step_score', 'N/A')}",
        f"Expectation Setting       : {a.get('expectation_setting_score', 'N/A')}",
        f"Offer Delivery            : {a.get('offer_delivery_score', 'N/A')}",
        f"Objection Handling (Dec)  : {a.get('objection_handling_declined_score', 'N/A')}",
        f"Urgency Anchor            : {a.get('urgency_anchor_score', 'N/A')}",
        f"Personal Connector        : {a.get('personal_connector_score', 'N/A')}",
        f"Call Recap                : {a.get('call_recap_score', 'N/A')}",
        f"Timing & Urgency          : {a.get('timing_urgency_score', 'N/A')}",
        f"New Personal Intel        : {a.get('new_personal_intel_score', 'N/A')}",
        f"Next Follow Up Set        : {a.get('next_followup_set_score', 'N/A')}",
        "",
        "─── FEEDBACK & RECOMMENDATIONS ───",
        a.get("rep_feedback", ""),
        "",
        "─── RAPPORT SUMMARY ───",
        a.get("rapport_connection_summary", ""),
        "",
        "─── OBJECTION BOXING ───",
        a.get("objection_boxing_result", ""),
        "",
        "─── INCOMPLETE CALL REASON ───",
        a.get("incomplete_call_reason", ""),
        "",
        "─── COACHING SUMMARY ───",
        a.get("coaching_summary_for_slack", ""),
        "",
        "─── NEXT BEST ACTION ───",
        a.get("next_best_action", ""),
    ])

    missed = a.get("missed_questions", [])
    if missed:
        lines += ["", "─── MISSED QUESTIONS ───"]
        lines += [f"• {q}" for q in missed]

    return "\n".join(lines)


async def _post_to_salesforce_chatter(lead_id: str, analysis: dict, record: dict):
    access_token = await get_sf_access_token()
    chatter_url  = f"{SF_INSTANCE_URL}/services/data/v57.0/chatter/feed-elements"
    body_text    = _build_chatter_body(analysis, record)

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


async def _send_email(analysis: dict, record: dict) -> None:
    """
    Send call analysis email via Resend API to configured recipients.
    Uses the same formatted content as Salesforce Chatter.
    """
    try:
        body_text = _build_chatter_body(analysis, record)

        resend.api_key = os.environ.get("RESEND_API_KEY")

        resend.Emails.send({
            "from": "onboarding@resend.dev",
            "to": _EMAIL_RECIPIENTS,
            "subject": _EMAIL_SUBJECT,
            "text": body_text,
        })

        logger.info("Email sent to %s | subject=%s", ", ".join(_EMAIL_RECIPIENTS), _EMAIL_SUBJECT)

    except Exception as exc:
        logger.error("Failed to send email: %s", exc)


def _get_sheets_client() -> gspread.Client:
    creds_source = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if creds_source.strip().startswith("{"):
        try:
            service_account_info = json.loads(creds_source)
            service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        except json.JSONDecodeError as exc:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON contains invalid JSON") from exc
        creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    else:
        creds = Credentials.from_service_account_file(creds_source, scopes=scopes)

    return gspread.authorize(creds)


# updated headers to match new column order and names
_SHEET_HEADERS = [
    "Call ID",
    "Record ID",
    "Call Link",
    "Call From",
    "Call To",
    "Lead Owner",
    "Opportunity Owner",
    "Call Type",
    "Timestamp",
    "Duration",
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
    "Seller Motivation",
    "Seller Urgency",
    "Property Condition",
    "Expectation Setting Score",
    "Rapport & Connection Score",
    "Offer Delivery Score",
    "Objection Handling Declined Score",
    "Urgency Anchor Score",
    "Clear Next Step Score",
    "Objection Boxing Result",
    "Incomplete Call Reason",
    "Personal Connector Score",
    "Call Recap Score",
    "Timing & Urgency Score",
    "New Personal Intel Score",
    "Next Follow Up Set Score",
    "Call Summary",
    "Rep Feedback",
    "Rapport Connection Summary",
    "Price Notes",
    "Next Best Action",
    "Coaching Summary For Slack",
    "Missed Questions",
    "Call Transcript",
]


def _ensure_header_row(worksheet: gspread.Worksheet) -> None:
    first_row = worksheet.row_values(1)
    if not first_row or first_row[0] != "Call ID":
        worksheet.insert_row(_SHEET_HEADERS, index=1)
        logger.info("Header row written to '%s'", _SMRT_WORKSHEET_NAME)


def _append_to_google_sheet(record: dict, analysis: dict, transcript: str) -> None:
    try:
        gc = _get_sheets_client()
        sh = gc.open_by_key(_SMRT_SHEET_ID)

        try:
            ws = sh.worksheet(_SMRT_WORKSHEET_NAME)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=_SMRT_WORKSHEET_NAME, rows=1000, cols=50)
            logger.info("Created new worksheet '%s'", _SMRT_WORKSHEET_NAME)

    except Exception as exc:
        logger.error("Could not open Google Sheet: %s", exc)
        return

    _ensure_header_row(ws)

    a = analysis
    missed = "; ".join(a.get("missed_questions", []))
    now = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d %H:%M:%S PST")

    row = [
        record.get("call_id", ""),           # Call ID
        record.get("record_id", ""),          # Record ID (new)
        record.get("call_link", ""),          # Call Link (new)
        record.get("call_from", ""),          # Call From (renamed from caller)
        record.get("call_to", ""),            # Call To (new)
        record.get("lead_owner", ""),         # Lead Owner (new)
        record.get("opportunity_owner", ""),  # Opportunity Owner (new)
        a.get("call_type", ""),               # Call Type
        now,                                  # Timestamp
        record.get("duration", ""),           # Duration (new)
        a.get("overall_score", ""),           # Overall Score
        a.get("grade", ""),                   # Grade
        a.get("opening_score"),               # Opening Score
        a.get("going_deep_score"),            # Going Deep Score
        a.get("motivation_score"),            # Motivation Score
        a.get("urgency_score"),               # Urgency Score
        a.get("condition_score"),             # Condition Score
        a.get("price_score"),                 # Price Score
        a.get("objection_score"),             # Objection Score
        a.get("next_step_score"),             # Next Step Score
        a.get("seller_motivation", ""),       # Seller Motivation
        a.get("seller_urgency", ""),          # Seller Urgency
        a.get("property_condition", ""),      # Property Condition
        a.get("expectation_setting_score"),   # Expectation Setting Score
        a.get("rapport_connection_score"),    # Rapport & Connection Score
        a.get("offer_delivery_score"),        # Offer Delivery Score
        a.get("objection_handling_declined_score"),  # Objection Handling Declined Score
        a.get("urgency_anchor_score"),        # Urgency Anchor Score
        a.get("clear_next_step_score"),       # Clear Next Step Score
        a.get("objection_boxing_result", ""), # Objection Boxing Result
        a.get("incomplete_call_reason", ""),  # Incomplete Call Reason
        a.get("personal_connector_score"),    # Personal Connector Score
        a.get("call_recap_score"),            # Call Recap Score
        a.get("timing_urgency_score"),        # Timing & Urgency Score
        a.get("new_personal_intel_score"),    # New Personal Intel Score
        a.get("next_followup_set_score"),     # Next Follow Up Set Score
        a.get("call_summary", ""),            # Call Summary
        a.get("rep_feedback", ""),            # Rep Feedback
        a.get("rapport_connection_summary", ""),  # Rapport Connection Summary
        a.get("price_notes", ""),             # Price Notes
        a.get("next_best_action", ""),        # Next Best Action
        a.get("coaching_summary_for_slack", ""),  # Coaching Summary For Slack
        missed,                               # Missed Questions
        transcript,                           # Call Transcript
    ]

    ws.append_row(row, value_input_option="USER_ENTERED")

    logger.info(
        "Sheet row appended | call_id=%s | sheet=%s | tab=%s",
        record.get("call_id"),
        _SMRT_SHEET_ID,
        _SMRT_WORKSHEET_NAME,
    )