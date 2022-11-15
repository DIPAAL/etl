from etl.audit.logger import AuditLogger
from datetime import datetime, timedelta
import os
import pytest


VERSION_NUMBERS_LIST = ['v1.0.2', 'version 2', 'final final', '2nd prototype', None, 'v1.22.12']

@pytest.mark.parametrize('version_number', VERSION_NUMBERS_LIST)
def test_audit_log_version_number(version_number):
    al = AuditLogger()

    al.log_etl_version(version_number)

    assert al.log_dict['etl_version'] == version_number

def test_audit_log_etl_stage():
    al = AuditLogger()
    start_time = 0
    end_time = 42
    expected_time = end_time - start_time
    expected_rows = 100
    start_datetime = datetime(2022, 11, 15, 11, 25, 0)
    end_datetime = datetime(2022, 11, 15, 11, 25, 22)
    expected_datetime = timedelta.total_seconds(end_datetime - start_datetime)

    al.log_etl_stage('cleaning', start_time, end_time, expected_rows)
    al.log_etl_stage('spatial_join', start_datetime, end_datetime, expected_rows)

    assert al.log_dict['cleaning_delta_time'] == expected_time
    assert al.log_dict['cleaning_rows'] == expected_rows
    assert al.log_dict['spatial_join_delta_time'] == expected_datetime
    assert al.log_dict['spatial_join_rows'] == expected_rows

    al.log_total_delta_time()
    assert al.log_dict['total_delta_time'] == expected_time + expected_datetime


TEST_FILES = ['tests/data/ferry.csv', 'tests/data/clean_df.csv']

@pytest.mark.parametrize('file_path', TEST_FILES)
def test_audit_log_file(file_path):
    al = AuditLogger()
    al.log_file(file_path)

    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    file_rows = len(open(file_path).readlines())

    assert al.log_dict['file_name'] == file_name
    assert al.log_dict['file_size'] == file_size
    assert al.log_dict['file_rows'] == file_rows

