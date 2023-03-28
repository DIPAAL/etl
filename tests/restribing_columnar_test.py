import pytest

from datetime import datetime
from etl.insert.restribing_columnar import _is_last_day_of_month, _get_partition_name

_is_last_day_of_month_testdata = [
    (datetime(2022, 1, 1), False),
    (datetime(2022, 1, 31), True),
    (datetime(2022, 2, 15), False),
    (datetime(2022, 2, 28), True),
    (datetime(2024, 2, 29), True),
    (datetime(2022, 6, 30), True)
]


@pytest.mark.parametrize('date, expected_result', _is_last_day_of_month_testdata)
def test_is_last_day_of_month(date: datetime, expected_result: bool):
    assert expected_result == _is_last_day_of_month(date)


_get_partition_name_testdata = [
    ('test_table', datetime(2022, 10, 1), 'test_table_2022_10'),
    ('test_table', datetime(2022, 10, 31), 'test_table_2022_10'),
    ('test_table', datetime(2022, 10, 1), 'test_table_2022_10'),
    ('test_table', datetime(2021, 9, 1), 'test_table_2021_09'),
    ('test_2_table', datetime(2021, 4, 25), 'test_2_table_2021_04'),
]

@pytest.mark.parametrize('table_name, cur_date, expected_result', _get_partition_name_testdata)
def test_get_partition_name(table_name: str, cur_date: datetime, expected_result: str):
    assert expected_result == _get_partition_name(table_name, cur_date)