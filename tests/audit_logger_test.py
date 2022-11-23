from etl.audit.logger import AuditLogger
from datetime import datetime, timedelta
import os
import pytest
import pandas as pd
import geopandas as gpd
import numpy as np


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
    dataframe_pandas = pd.DataFrame(index=np.arange(0, expected_rows), columns=['col1', 'col2', 'col3'])
    dataframe_geopandas = gpd.GeoDataFrame(index=np.arange(0, expected_rows), columns=['col1', 'col2', 'col3'])

    al.log_etl_stage_time('cleaning', start_time, end_time)
    al.log_etl_stage_rows_df('cleaning', dataframe_pandas)
    al.log_etl_stage_time('spatial_join', start_datetime, end_datetime)
    al.log_etl_stage_rows_df('spatial_join', dataframe_geopandas)

    assert al.log_dict['cleaning_delta_time'] == expected_time
    assert al.log_dict['cleaning_rows'] == expected_rows
    assert al.log_dict['spatial_join_delta_time'] == expected_datetime
    assert al.log_dict['spatial_join_rows'] == expected_rows
    assert al.log_dict['total_delta_time'] == expected_time + expected_datetime

    al.reset_log()
    al.configure_log_settings(log_etl_stage_time=False, log_etl_stage_rows=False)
    al.log_etl_stage_time('cleaning', start_time, end_time)
    al.log_etl_stage_rows_df('cleaning', dataframe_pandas)
    al.log_etl_stage_time('spatial_join', start_datetime, end_datetime)
    al.log_etl_stage_rows_df('spatial_join', dataframe_geopandas)

    assert al.log_dict['cleaning_delta_time'] is None
    assert al.log_dict['cleaning_rows'] is None
    assert al.log_dict['spatial_join_delta_time'] is None
    assert al.log_dict['spatial_join_rows'] is None


def test_audit_log_etl_stage_time_raises_error():
    with pytest.raises(ValueError):
        al = AuditLogger()
        al.log_etl_stage_time('invalid_stage_name', 0, 42)


def test_audit_log_etl_stage_rows_df_raises_error():
    with pytest.raises(ValueError):
        al = AuditLogger()
        al.log_etl_stage_rows_df('invalid_stage_name', pd.DataFrame())


def test_audit_log_etl_stage_rows_df_raises_type_error():
    with pytest.raises(TypeError):
        al = AuditLogger()
        al.log_etl_stage_rows_df('cleaning', None)


def test_audit_log_etl_stage_rows_cursor_raises_name_error():
    with pytest.raises(ValueError):
        al = AuditLogger()
        al.log_etl_stage_rows_cursor('invalid_stage_name', None)


def test_audit_log_etl_stage_rows_cursor_raises_attribute_error():
    with pytest.raises(AttributeError):
        al = AuditLogger()
        al.log_etl_stage_rows_cursor('cleaning', None)


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

    assert al.log_dict['file_name'] == file_name
    assert al.log_dict['file_size'] == file_size
    assert al.log_dict['file_rows'] == file_rows

    al.configure_log_settings(log_file_rows=False)

    al.reset_log()
    al.log_file(file_path)

    assert al.log_dict['file_name'] == file_name
    assert al.log_dict['file_size'] == file_size
    assert al.log_dict['file_rows'] is None
