from config.config import ELEVEN_LABS_KEY, ELEVENLABS_URL
import requests, logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(funcName)s | %(message)s"
)

def make_call(phone_id, to_number, address):
    logging.info(f"Making call | to: {to_number}, from_id: {phone_id}, address: {address}")

    payload = {
        "agent_id": "agent_6401kkp1zhqketvbbkqhc9jgnh4c",
        "agent_phone_number_id": phone_id,
        "to_number": to_number,
        "conversation_initiation_client_data": {
            "dynamic_variables": {
                "address": address
            }
        }
    }

    headers = {
        "xi-api-key": ELEVEN_LABS_KEY,
        "Content-Type": "application/json"
    }

    res = requests.post(ELEVENLABS_URL, json=payload, headers=headers)
    logging.info(f"ElevenLabs status code: {res.status_code}")
    try:
        response_json = res.json()
        logging.info(f"ElevenLabs response: {response_json}")
        return response_json
    except Exception:
        logging.error("Failed to parse ElevenLabs response")
        return {}