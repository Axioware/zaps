import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(funcName)s | %(message)s"
)
def remove_plus(phone):
    cleaned = phone.lstrip("+")
    logging.info(f"Removed plus: {phone} -> {cleaned}")
    return cleaned


