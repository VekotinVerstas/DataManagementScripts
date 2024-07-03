import datetime

import pytest

from fvhdms.utils.time import parse_times


def test_parse_times_with_period_year():
    start_time, end_time, duration = parse_times(None, None, None, period="2024")
    assert start_time == datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    assert end_time == datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
    assert duration == 366 * 24 * 60 * 60  # 366 days


def test_parse_times_with_period_month():
    start_time, end_time, duration = parse_times(None, None, None, period="2024-06")
    assert start_time == datetime.datetime(2024, 6, 1, tzinfo=datetime.timezone.utc)
    assert end_time == datetime.datetime(2024, 7, 1, tzinfo=datetime.timezone.utc)
    assert duration == 30 * 24 * 60 * 60  # 30 days


def test_parse_times_with_period_day():
    start_time, end_time, duration = parse_times(None, None, None, period="2024-06-30")
    assert start_time == datetime.datetime(2024, 6, 30, tzinfo=datetime.timezone.utc)
    assert end_time == datetime.datetime(2024, 7, 1, tzinfo=datetime.timezone.utc)
    assert duration == 24 * 60 * 60  # 1 day


def test_parse_times_with_duration():
    end_time = datetime.datetime(2024, 6, 30, tzinfo=datetime.timezone.utc)
    start_time, end_time, duration = parse_times(None, end_time, "P1D", None)
    assert start_time == end_time - datetime.timedelta(days=1)
    assert duration == 24 * 60 * 60  # 1 day


def test_parse_times_with_start_and_end_time():
    start_time = datetime.datetime(2024, 6, 29, tzinfo=datetime.timezone.utc)
    end_time = datetime.datetime(2024, 6, 30, tzinfo=datetime.timezone.utc)
    parsed_start_time, parsed_end_time, duration = parse_times(start_time, end_time, None, None)
    assert parsed_start_time == start_time
    assert parsed_end_time == end_time
    assert duration == 24 * 60 * 60  # 1 day


def test_parse_times_with_subtract_end_time():
    end_time = datetime.datetime(2024, 6, 30, tzinfo=datetime.timezone.utc)
    start_time, end_time, duration = parse_times(None, end_time, "P1D", None, subtract_end_time=3600)
    assert end_time == datetime.datetime(2024, 6, 29, 23, 0, tzinfo=datetime.timezone.utc)
    assert duration == 23 * 60 * 60  # 23 hours


def test_parse_times_with_round_times():
    end_time = datetime.datetime(2024, 6, 30, 15, 45, tzinfo=datetime.timezone.utc)
    start_time, end_time, duration = parse_times(None, end_time, "P1D", None, round_times=True)
    assert end_time == datetime.datetime(2024, 6, 30, 15, 0, tzinfo=datetime.timezone.utc)
    assert start_time == end_time - datetime.timedelta(days=1)
    assert duration == 24 * 60 * 60  # 1 day


def test_parse_times_invalid_period_format():
    with pytest.raises(ValueError):
        parse_times(None, None, None, period="invalid")


def test_parse_times_no_inputs():
    with pytest.raises(RuntimeError):
        parse_times(None, None, None, None)


def test_parse_times_start_after_end():
    start_time = datetime.datetime(2024, 7, 1, tzinfo=datetime.timezone.utc)
    end_time = datetime.datetime(2024, 6, 30, tzinfo=datetime.timezone.utc)
    with pytest.raises(ValueError):
        parse_times(start_time, end_time, None, None)


if __name__ == "__main__":
    pytest.main()
