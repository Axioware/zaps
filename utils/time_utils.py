from datetime import datetime

def parse_time(t: str):
    if not t:
        return None

    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(t, fmt).time()
        except ValueError:
            continue

    raise ValueError(f"Invalid time format: {t}")


def is_within_time_window(start_time: str, end_time: str):
    now = datetime.now().time()

    start = parse_time(start_time)
    end = parse_time(end_time)

    if start < end:
        return start <= now <= end
    else:
        # overnight case
        return now >= start or now <= end