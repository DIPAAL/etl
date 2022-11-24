from etl.audit.logger import AuditLogger
import os
import pytest
import pandas as pd
import geopandas as gpd
import numpy as np
from etl.constants import ETL_STAGE_CLEAN, ETL_STAGE_SPATIAL


def test_audit_log_version_number():
    al = AuditLogger()

    if os.getenv('tag'):
        os.unsetenv('tag')
    al.log_etl_version()

    assert al.log_dict['etl_version'] == 'local_dev'

    al.reset_log()
    expected_tag = 'v1.0.0'
    os.environ['tag'] = expected_tag
    al.log_etl_version()

    assert al.log_dict['etl_version'] == expected_tag


def test_audit_log_etl_stage():
    al = AuditLogger()
    start_time = 0
    end_time = 42.320393
    expected_time = end_time - start_time
    expected_rows = 100
    dataframe_pandas = pd.DataFrame(index=np.arange(0, expected_rows), columns=['col1', 'col2', 'col3'])
    dataframe_geopandas = gpd.GeoDataFrame(index=np.arange(0, expected_rows), columns=['col1', 'col2', 'col3'])

    al.log_etl_stage_time(ETL_STAGE_CLEAN, start_time, end_time)
    al.log_etl_stage_rows_df(ETL_STAGE_CLEAN, dataframe_pandas)
    al.log_etl_stage_time(ETL_STAGE_SPATIAL, start_time, end_time)
    al.log_etl_stage_rows_df(ETL_STAGE_SPATIAL, dataframe_geopandas)

    assert al.log_dict[f'{ETL_STAGE_CLEAN}_delta_time'] == expected_time
    assert al.log_dict[f'{ETL_STAGE_CLEAN}_rows'] == expected_rows
    assert al.log_dict[f'{ETL_STAGE_SPATIAL}_delta_time'] == expected_time
    assert al.log_dict[f'{ETL_STAGE_SPATIAL}_rows'] == expected_rows
    assert al.log_dict['total_delta_time'] == expected_time * 2

    al.reset_log()
    al.log_rows_false()
    al.log_etl_stage_rows_df(ETL_STAGE_CLEAN, dataframe_pandas)

    assert al.log_dict[f'{ETL_STAGE_CLEAN}_rows'] is None


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
        al.log_etl_stage_rows_df(ETL_STAGE_CLEAN, None)


def test_audit_log_etl_stage_rows_cursor_raises_name_error():
    with pytest.raises(ValueError):
        al = AuditLogger()
        al.log_etl_stage_rows_cursor('invalid_stage_name', None)


def test_audit_log_etl_stage_rows_cursor_raises_attribute_error():
    with pytest.raises(AttributeError):
        al = AuditLogger()
        al.log_etl_stage_rows_cursor(ETL_STAGE_CLEAN, None)


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

    al.log_rows_false()

    al.reset_log()
    al.log_file(file_path)

    assert al.log_dict['file_name'] == file_name
    assert al.log_dict['file_size'] == file_size
    assert al.log_dict['file_rows'] is None
