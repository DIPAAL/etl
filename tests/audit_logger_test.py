from etl.audit.logger import AuditLogger
from datetime import datetime, timedelta

def test_audit_logger_version_number():
    al = AuditLogger()
    version_name = 'test_version'
    version_number = 'v2.3.5'

    al.log_version(version_name, version_number)

    assert al.dict_version[version_name] == version_number

    version_number_major = 2
    version_number_middle = 3
    version_number_minor = 5
    expected_version_number = [version_number_minor, version_number_middle, version_number_major]

    # assert al.get_version_values(version_name) == expected_version_number FIXME: Make into parameterized test

def test_audit_logger_process_time():
    al = AuditLogger()
    process_name = 'test_process'
    process_start_time = 0
    process_end_time = 25
    process_rows = 100

    al.log_process(process_name, process_start_time, process_end_time, process_rows)

    expected_timespan = process_end_time - process_start_time

    assert al.dict_process[process_name]['process_timespan'] == expected_timespan
    assert al.dict_process[process_name]['process_rows'] == process_rows

    process_start_time = datetime.now()
    process_end_time = datetime.now()+timedelta(seconds=420)

    al.log_process(process_name, process_start_time, process_end_time, process_rows)

    expected_timespan = timedelta.total_seconds(process_end_time - process_start_time)

    assert al.dict_process[process_name]['process_timespan'] == expected_timespan
