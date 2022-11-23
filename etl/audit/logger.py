"""Module responsible for logging details about the execution of each stage of the ETL process."""
from datetime import datetime, timedelta

import os
import pandas as pd
import geopandas as gpd
from typing import Union
from etl.constants import ETL_PROJECT_VERSION, ETL_STAGE_CLEAN, ETL_STAGE_SPATIAL, ETL_STAGE_TRAJECTORY, \
    ETL_STAGE_CELL, ETL_STAGE_BULK


class AuditLogger:
    """Class responsible for logging details about the execution of each stage of the ETL process.

    Attributes:
        log_dict (dict): dictionary containing the logs
        _log_settings (dict): dictionary containing the log settings
    """

    def __init__(self):
        """
        Construct an instance of the AuditLogger class.

        log_dict: dictionary containing the logs
        _log_settings: dictionary containing the log settings
        """

        self.log_dict = {
            'import_datetime': datetime.now(),
            'requirements': [],
            'etl_version': None,

            'file_name': None,
            'file_size': None,
            'file_rows': None,

            f'{ETL_STAGE_CLEAN}_delta_time': None,
            f'{ETL_STAGE_CLEAN}_rows': None,

            f'{ETL_STAGE_SPATIAL}_delta_time': None,
            f'{ETL_STAGE_SPATIAL}_rows': None,

            f'{ETL_STAGE_TRAJECTORY}_delta_time': None,
            f'{ETL_STAGE_TRAJECTORY}_rows': None,

            f'{ETL_STAGE_CELL}_delta_time': None,
            f'{ETL_STAGE_CELL}_rows': None,

            f'{ETL_STAGE_BULK}_delta_time': None,
            f'{ETL_STAGE_BULK}_rows': None,

            'total_delta_time': None,
        }

        self.log_file_rows = True
        self.log_etl_stage_rows = True

    def log_etl_stage_time(self, stage_name: str, stage_start_time, stage_end_time):
        """Log the time and number of rows of a given ETL stage.

        Keyword arguments:
            stage_name: name of the ETL stage, must be one of the following:
                'cleaning', 'spatial_join', 'trajectory', 'cell_construct', 'bulk_insert'
            stage_start_time: start time of the ETL stage
            stage_end_time: end time of the ETL stage
        """
        self._validate_stage_name(stage_name)

        time_delta = stage_end_time - stage_start_time

        self.log_dict[stage_name + '_delta_time'] = time_delta
        self._log_total_delta_time()

    def _log_total_delta_time(self):
        """Calculate the total time of the ETL process."""
        suffix = '_delta_time'
        self.log_dict['total_delta_time'] = sum([self.log_dict[key] for key in self.log_dict
                                                 if key.endswith(suffix)
                                                 and key != 'total_delta_time'
                                                 and self.log_dict[key] is not None])

    def log_etl_stage_rows_df(self, stage_name: str, stage_df: Union[pd.DataFrame, gpd.GeoDataFrame]):
        """Log the number of rows of a given ETL stage.

        Keyword arguments:
            stage_name: name of the ETL stage, must be one of the following:
                'cleaning', 'spatial_join', 'trajectory', 'cell_construct', 'bulk_insert'
            stage_df: dataframe for the ETL stage
        """
        self._validate_stage_name(stage_name)

        if self.log_etl_stage_rows:
            if isinstance(stage_df, (pd.DataFrame, gpd.GeoDataFrame)):
                self.log_dict[stage_name + '_rows'] = len(stage_df.index)
            else:
                raise TypeError(f'Invalid type for stage_df: {type(stage_df)}')

    def log_etl_stage_rows_cursor(self, stage_name: str, cursor):
        """Log the number of rows of a given ETL stage.

        Keyword arguments:
            stage_name: name of the ETL stage, must be one of the following:
                'cleaning', 'spatial_join', 'trajectory', 'cell_construct', 'bulk_insert'
            cursor: cursor object for the ETL stage
        """
        self._validate_stage_name(stage_name)
        if self.log_etl_stage_rows:
            try:
                if self.log_dict[stage_name + '_rows'] is None:
                    self.log_dict[stage_name + '_rows'] = cursor.rowcount
                else:
                    self.log_dict[stage_name + '_rows'] += cursor.rowcount  # += as bulk insert is called multiple times
            except AttributeError:
                raise AttributeError(f'Invalid cursor object: {type(cursor)}')

    def _validate_stage_name(self, stage_name):
        """Check if the stage name is valid.

        Keyword arguments:
            stage_name: name of the ETL stage, must be one of the following:
                'cleaning', 'spatial_join', 'trajectory', 'cell_construct', 'bulk_insert'
        """
        if stage_name + '_rows' not in self.log_dict:
            raise ValueError(f'Invalid name for ETL stage: {stage_name}')

    def log_etl_version(self, etl_version: str):
        """Log the version of the ETL process.

        Keyword arguments:
            etl_version: version of the ETL process
        """
        self.log_dict['etl_version'] = etl_version

    def log_file(self, file_path):
        """Log the file name, size and number of rows.

        Keyword arguments:
            file_path: path to the file
        """
        self.log_dict['file_name'] = os.path.basename(file_path)
        self.log_dict['file_size'] = os.path.getsize(file_path)
        if self.log_file_rows:
            self.log_dict['file_rows'] = self._get_file_rows(file_path)

    @staticmethod
    def _get_file_rows(file_path):
        """Return the number of rows of a given file.

        Keyword arguments:
            file_path: path to the file
        """
        with open(file_path, 'r') as f:
            for count, lines in enumerate(f):
                pass
        return count + 1

    def log_requirements(self, requirements_path='requirements.txt'):
        """Log the requirements of the ETL process.

        Keyword arguments:
            requirements_path: path to the requirements file
                (default: 'requirements.txt')
        """
        requirements = []
        for line in open(requirements_path, 'r'):
            if line.startswith('#'):
                continue
            requirements.append(line.strip())
        self.log_dict['requirements'] = requirements

    def log_rows_false(self):
        """Configure the log so that it seizes to log row counts for file and ETL stages.
        Should only be used if performance is an issue."""
        self.log_file_rows = False
        self.log_etl_stage_rows = False

    def reset_log(self):
        """Reset the log dictionary."""
        for key in self.log_dict:
            self.log_dict[key] = None
        self.log_dict['import_datetime'] = datetime.now()
        self.log_dict['requirements'] = []

    def to_dataframe(self):
        """Return a pandas DataFrame containing the logs."""
        df = pd.DataFrame.from_dict(self.log_dict, orient='index').T
        return df

    def get_logs_dict(self):
        """Return a dictionary containing the logs."""
        return self.log_dict


# Global audit logger class object, for storing logs.
global_audit_logger = AuditLogger()
global_audit_logger.log_etl_version(ETL_PROJECT_VERSION)


