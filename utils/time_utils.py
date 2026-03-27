from datetime import datetime

def is_within_time_window(start_time: str, end_time: str):
    now = datetime.now().time()

    start = datetime.strptime(start_time, "%H:%M").time()
    end = datetime.strptime(end_time, "%H:%M").time()

    return start <= now <= end