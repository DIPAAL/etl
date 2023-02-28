from etl.audit.logger import AuditLogger, ROWS_KEY, STATS_KEY
import os
import pytest


def test_audit_log_version_number():
    al = AuditLogger()

    if os.getenv('tag'):
        del os.environ['tag']
    al._log_etl_version()

    assert al._log_dict['etl_version'] == 'local_dev'

    al.reset_log()
    expected_tag = 'v1.0.0'
    os.environ['tag'] = expected_tag
    al._log_etl_version()

    assert al._log_dict['etl_version'] == expected_tag


def test_audit_log_file_raises_error():
    with pytest.raises(FileNotFoundError):
        al = AuditLogger()
        al.log_file('invalid_file_path')


TEST_FILES = ['tests/data/ferry.csv', 'tests/data/clean_df.csv']


@pytest.mark.parametrize('file_path', TEST_FILES)
def test_audit_log_file(file_path):
    al = AuditLogger()
    al.log_file(file_path)

    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    file_rows = len(open(file_path).readlines())  # Loads entire file into memory but fast

    assert al._log_dict['file_name'] == file_name
    assert al._log_dict['file_size'] == file_size
    assert al._log_dict[STATS_KEY][ROWS_KEY]['file'] == file_rows
