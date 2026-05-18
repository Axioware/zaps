import json
import os
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from datetime import datetime

# ── Load ENV ────────────────────────────────────────────────────────────────
load_dotenv()

# ── Google Auth ─────────────────────────────────────────────────────────────
creds_source = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

if not creds_source:
    raise Exception("GOOGLE_SERVICE_ACCOUNT_JSON not found in .env")

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

info = json.loads(creds_source)
info["private_key"] = info["private_key"].replace("\\n", "\n")

creds = Credentials.from_service_account_info(
    info,
    scopes=scopes
)

gc = gspread.authorize(creds)

# ── SHEET CONFIG ────────────────────────────────────────────────────────────
SHEET_ID = "1s6yeCkP6EXx-csiGMiyXAIRu73QoMFDXBzrQHsOT2vY"

# Your current Apps Script uses:
# var LMI_SHEET_NAME = "FI";

WORKSHEET_NAME = "FI"

# ── OPEN SHEET ──────────────────────────────────────────────────────────────
print("📊 Opening Google Sheet...")

sh = gc.open_by_key(SHEET_ID)
ws = sh.worksheet(WORKSHEET_NAME)

print(f"✅ Connected to worksheet: {WORKSHEET_NAME}")

# ── CREATE TEST ROW ─────────────────────────────────────────────────────────
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

test_row = [
    now,                               # Date
    "+13105614025",                   # Phone Number
    "123 Main Street, Dallas TX",     # Property Address
    "johnseller@example.com",         # Email
    "John",                           # First Name
    "Smith",                          # Last Name
    "Facebook Ads",                   # Lead Source
    "Yes",                            # Property Listed
    "30 Days",                        # Selling Timeline
    "Good Condition",                 # Property Condition
    "$450,000",                       # Property Worth
    "$400,000",                       # Fair Price
    "Relocating",                     # Selling Reason
    "facebook",                       # UTM Source
    "seller_campaign_01",             # UTM Campaign
    "TF-CERT-123456",                 # Trusted Form Cert
    "sell my house fast",             # Keywords
    "Texas Sellers Audience",         # Ad Set Name
    "Fast Cash Offer Ad"              # Ad Name
]

# ── APPEND ROW ──────────────────────────────────────────────────────────────
print("📝 Appending new row...")

ws.append_row(
    test_row,
    value_input_option="USER_ENTERED"
)

print("")
print("✅ SUCCESS!")
print("")
print("🔄 Workflow Check:")
print("1. Google Sheet  → New row added")
print("2. Apps Script   → onChangeLMI triggered")
print("3. Zapier        → Webhook received")
print("4. Salesforce    → Lead created/updated")
print("5. n8n           → Workflow executed")

# import json
# import os
# import gspread
# from dotenv import load_dotenv
# from google.oauth2.service_account import Credentials

# load_dotenv()

# creds_source = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

# info = json.loads(creds_source)
# info["private_key"] = info["private_key"].replace("\\n", "\n")

# scopes = [
#     "https://www.googleapis.com/auth/spreadsheets",
#     "https://www.googleapis.com/auth/drive",
# ]

# creds = Credentials.from_service_account_info(info, scopes=scopes)

# gc = gspread.authorize(creds)

# SHEET_ID = "1s6yeCkP6EXx-csiGMiyXAIRu73QoMFDXBzrQHsOT2vY"

# sh = gc.open_by_key(SHEET_ID)

# print("\n📋 Available Worksheets:\n")

# for ws in sh.worksheets():
#     print("-", ws.title)