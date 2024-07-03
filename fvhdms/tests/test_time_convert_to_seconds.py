import pytest

from fvhdms.utils.time import convert_to_seconds


def test_convert_to_seconds_seconds():
    assert convert_to_seconds("500s") == 500
    assert convert_to_seconds("0s") == 0


def test_convert_to_seconds_minutes():
    assert convert_to_seconds("1m") == 60
    assert convert_to_seconds("120m") == 7200


def test_convert_to_seconds_hours():
    assert convert_to_seconds("1h") == 3600
    assert convert_to_seconds("24h") == 86400


def test_convert_to_seconds_days():
    assert convert_to_seconds("1d") == 86400
    assert convert_to_seconds("5d") == 432000


def test_convert_to_seconds_weeks():
    assert convert_to_seconds("1w") == 604800
    assert convert_to_seconds("16w") == 9676800


def test_convert_to_seconds_invalid_unit():
    with pytest.raises(RuntimeError, match="Invalid time period: 10y, use postfixes s, m, h, d, w"):
        convert_to_seconds("10y")


def test_convert_to_seconds_no_unit():
    with pytest.raises(RuntimeError, match="Invalid time period: 100, use postfixes s, m, h, d, w"):
        convert_to_seconds("100")


def test_convert_to_seconds_empty_string():
    with pytest.raises(ValueError):
        convert_to_seconds("")


def test_convert_to_seconds_non_numeric():
    with pytest.raises(ValueError):
        convert_to_seconds("xs")
