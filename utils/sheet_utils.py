import re

def extract_sheet_id(sheet_url: str) -> str:
    match = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_url)
    return match.group(1) if match else None