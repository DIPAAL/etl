from datetime import datetime

import pytest

from etl.gatherer.file_downloader import date_from_filename


def test_it_transforms_file_name_to_datetime():
    file_name = 'aisdk-2007-04.zip'
    expected = datetime(year=2007, month=4, day=1)
    result = date_from_filename(file_name)
    assert expected == result

    file_name = 'aisdk-2007-04-03.zip'
    expected = datetime(year=2007, month=4, day=3)
    result = date_from_filename(file_name)
    assert expected == result

    file_name = 'aisdk-2007-04-03.rar'
    expected = datetime(year=2007, month=4, day=3)
    result = date_from_filename(file_name)
    assert expected == result

    file_name = 'invalid-file-name'
    # assert it raises an exception
    with pytest.raises(Exception):
        date_from_filename(file_name)
