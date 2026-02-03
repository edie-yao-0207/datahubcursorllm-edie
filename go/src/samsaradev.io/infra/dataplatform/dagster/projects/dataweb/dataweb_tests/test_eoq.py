from datetime import datetime

from dataweb.userpkgs.utils import get_trailing_end_of_quarter_dates


def test_get_trailing_end_of_quarter_dates():
    quarter_ends = get_trailing_end_of_quarter_dates("2024-12-30")
    assert quarter_ends == [
        "2024-03-31",
        "2024-06-30",
        "2024-09-30",
        "2023-12-31",
    ], f"quarter_ends {quarter_ends} is not correct"

    quarter_ends_2 = get_trailing_end_of_quarter_dates("2024-12-31")
    assert quarter_ends_2 == [
        "2024-12-31",
        "2024-03-31",
        "2024-06-30",
        "2024-09-30",
    ], f"quarter_ends {quarter_ends_2} is not correct"

    return
