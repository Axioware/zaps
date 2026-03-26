import os
from dotenv import load_dotenv

load_dotenv()

# ------------------- ENV VALIDATION -------------------

def get_env(key: str, required: bool = True, default=None):
    value = os.getenv(key, default)
    
    if required and not value:
        raise RuntimeError(f"❌ Missing required environment variable: {key}")
    
    return value


# ------------------- CONFIG CLASS -------------------

class Settings:
    # ================= ADMIN =================
    ADMIN_SECRET_KEY: str = get_env("ADMIN_SECRET_KEY")

    # ================= SALESFORCE =================
    SF_REFRESH_TOKEN: str = get_env("SF_REFRESH_TOKEN")
    SF_INSTANCE_URL: str = get_env("SF_INSTANCE_URL")
    SF_CLIENT_ID: str = get_env("SF_CLIENT_ID")
    SF_CLIENT_SECRET: str = get_env("SF_CLIENT_SECRET")

    # ================= ELEVENLABS =================
    ELEVEN_LABS_KEY: str = get_env("ELEVEN_LABS_KEY")
    ELEVEN_AGENT_ID: str = get_env("ELEVEN_AGENT_ID")

    # ================= SHEETS (ALAB) =================
    ALAB_SPREADSHEET_NAME: str = get_env("ALAB_SPREADSHEET_NAME")
    ALAB_WORKSHEET_NAME: str = get_env("ALAB_WORKSHEET_NAME")

    # ================= MAPPER SHEET =================
    MAPPER_SHEET_NAME: str = get_env("MAPPER_SHEET_NAME")
    MAPPER_WORKSHEET_NAME: str = get_env("MAPPER_WORKSHEET_NAME")


# Create a single config instance
settings = Settings()


# ------------------- SAFE EXPORTS -------------------

ADMIN_SECRET_KEY = settings.ADMIN_SECRET_KEY

SF_REFRESH_TOKEN = settings.SF_REFRESH_TOKEN
SF_INSTANCE_URL = settings.SF_INSTANCE_URL
SF_CLIENT_ID = settings.SF_CLIENT_ID
SF_CLIENT_SECRET = settings.SF_CLIENT_SECRET

ELEVEN_LABS_KEY = settings.ELEVEN_LABS_KEY
ELEVEN_AGENT_ID = settings.ELEVEN_AGENT_ID


ALAB_SPREADSHEET_NAME = settings.ALAB_SPREADSHEET_NAME
ALAB_WORKSHEET_NAME = settings.ALAB_WORKSHEET_NAME

MAPPER_SHEET_NAME = settings.MAPPER_SHEET_NAME
MAPPER_WORKSHEET_NAME = settings.MAPPER_WORKSHEET_NAME


# ------------------- AREA CODE MAPPING -------------------

AREA_CODE_MAP = {
    '213': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '310': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '323': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '357': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '424': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '562': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '626': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '661': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '738': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '747': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '805': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '818': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',
    '820': 'phnum_6201khx5e97ged1sxvbtvdhexcqc',

    '209': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '279': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '341': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '350': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '408': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '415': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '510': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '530': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '559': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '628': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '650': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '669': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '707': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '831': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '837': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '916': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',
    '925': 'phnum_5401kjcdggekfgjrqd8281cm4wr3',

    '619': 'phnum_1101kjcdrvnjejwr4j3g008g264r',
    '858': 'phnum_1101kjcdrvnjejwr4j3g008g264r',

    '369': 'phnum_1001khx5g9zcfj1bdxk5mkr00ktw',
    '442': 'phnum_1001khx5g9zcfj1bdxk5mkr00ktw',
    '760': 'phnum_1001khx5g9zcfj1bdxk5mkr00ktw',
    '840': 'phnum_1001khx5g9zcfj1bdxk5mkr00ktw',
    '909': 'phnum_1001khx5g9zcfj1bdxk5mkr00ktw',
    '959': 'phnum_1001khx5g9zcfj1bdxk5mkr00ktw',

    '657': 'phnum_0001khx5hqh7fbkryh4ymgsnwns5',
    '714': 'phnum_0001khx5hqh7fbkryh4ymgsnwns5',
    '949': 'phnum_0001khx5hqh7fbkryh4ymgsnwns5',
}

DEFAULT_PHONE = 'phnum_7801k60w0n6vecav04d878ej4g7x'