from datetime import datetime

import pytest

from etl.constants import CVS_TIMESTAMP_FORMAT, UNKNOWN_INT_VALUE
from etl.helper_functions import extract_smart_date_id_from_date, extract_smart_time_id_from_date

test_data_date_smart_key_extraction = [
    (datetime.strptime('01/01/2022 00:00:00', CVS_TIMESTAMP_FORMAT), 20220101),
    (datetime.strptime('02/01/2022 00:00:00', CVS_TIMESTAMP_FORMAT), 20220102),
    (datetime.strptime('01/02/2022 00:00:00', CVS_TIMESTAMP_FORMAT), 20220201),
    (datetime.strptime('07/09/2021 00:00:00', CVS_TIMESTAMP_FORMAT), 20210907),
    (datetime.strptime('31/01/2022 10:10:20', CVS_TIMESTAMP_FORMAT), 20220131),  # Show that time does not matter
    (datetime.strptime('31/01/2022 13:14:15', CVS_TIMESTAMP_FORMAT), 20220131),  # Show that time does not matter
    (None, -1)
]


@pytest.mark.parametrize('date, expected_smart_key', test_data_date_smart_key_extraction)
def test_date_smart_key_extraction(date, expected_smart_key):
    assert extract_smart_date_id_from_date(date) == expected_smart_key


test_data_time_smart_key_extraction = [
    (datetime(year=2021, month=1, day=1), 0),
    (datetime(year=2021, month=1, day=1, hour=10), 100000),
    (datetime(year=2021, month=1, day=1, minute=18), 1800),
    (datetime(year=2021, month=1, day=1, second=42), 42),
    (datetime(year=2021, month=1, day=1, hour=11, minute=11, second=11), 111111),
    (datetime(year=2021, month=1, day=1, hour=4, minute=5, second=6), 40506),
    (None, UNKNOWN_INT_VALUE)
]


@pytest.mark.parametrize('date, expected_smart_key', test_data_time_smart_key_extraction)
def test_time_smart_key_extraction(date, expected_smart_key):
    assert extract_smart_time_id_from_date(date) == expected_smart_key
