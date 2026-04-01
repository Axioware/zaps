from repositories.google_sheets_repository import load_area_code_map
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(funcName)s | %(message)s"
)

AREA_CODE_CACHE = None


def get_area_code_map_cached():
    global AREA_CODE_CACHE
    if AREA_CODE_CACHE is None:
        AREA_CODE_CACHE = load_area_code_map()
    return AREA_CODE_CACHE


def get_area_mapping(area):
    area_map = get_area_code_map_cached()
    lis = area_map.get(area, None)

    if lis:
        return lis[0], lis[1]
    return None, None
