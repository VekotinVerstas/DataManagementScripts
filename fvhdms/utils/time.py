import datetime
import re
from typing import Union, Tuple

import isodate


def convert_to_seconds(s: str) -> int:
    """
    Convert string like 500s, 120m, 24h, 5d, 16w to equivalent number of seconds

    :param str s: time period length
    :return: seconds
    """
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    try:
        return int(s[:-1]) * units[s[-1]]
    except KeyError:
        raise RuntimeError(f"Invalid time period: {s}, use postfixes s, m, h, d, w")


def parse_times(
    start_time: Union[datetime.datetime, None],
    end_time: Union[datetime.datetime, None],
    duration: Union[str, None],
    period: Union[str, None] = None,
    subtract_end_time: float = 0.0,
    round_times: bool = False,
) -> Tuple[datetime.datetime, datetime.datetime, int]:
    """
    Parse time period's start and end time. If start time is not given, use end time minus duration.
    If subtract_end_time is given, subtract that amount of seconds from the end time.
    This is useful in some databases or APIs.
    """
    if start_time is None and duration is None and period is None:
        raise RuntimeError("Either start time or duration or period must be given")

    if period:
        date_regex = re.compile(r"(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?")
        match = date_regex.match(period)
        if not match:
            raise ValueError("Date string is not in the correct format")
        year = int(match.group(1))
        month = int(match.group(2) or 1)
        day = int(match.group(3) or 1)
        start_time = datetime.datetime(year, month, day, tzinfo=datetime.timezone.utc)

        if match.group(3):  # If day is present add 1 day to the start time
            end_time = start_time + datetime.timedelta(days=1)
        elif match.group(2):  # If only year & month are present add 1 month to the start time
            next_month = month % 12 + 1
            next_year = year + month // 12
            end_time = start_time.replace(year=next_year, month=next_month, day=1)
        else:  # Only year is present, add 1 year to the start time
            end_time = start_time.replace(year=year + 1)
    else:
        if end_time is None:
            end_time = datetime.datetime.now().astimezone(tz=datetime.timezone.utc)

        if round_times:
            end_time = end_time.replace(minute=0, second=0, microsecond=0)

        if start_time is None:
            duration_seconds = isodate.parse_duration(duration).total_seconds()
            if duration_seconds == 0:
                raise ValueError(f"Invalid duration string. {duration} results total_seconds == 0")
            start_time = end_time - datetime.timedelta(seconds=duration_seconds)

    if subtract_end_time != 0.0:
        end_time -= datetime.timedelta(seconds=subtract_end_time)

    if start_time >= end_time:
        raise ValueError("Start time must be before end time")

    return start_time, end_time, int((end_time - start_time).total_seconds())
