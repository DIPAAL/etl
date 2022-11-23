"""Module responsible for logging details about the execution of each stage of the ETL process."""
from datetime import datetime, timedelta

import os
import pandas as pd
import geopandas as gpd
from typing import Union


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
        now = datetime.now()
        self.log_dict = {
            'import_date': now.date(),
            'import_time': now.time(),
            'requirements': [],
            'etl_version': None,

            'file_name': None,
            'file_size': None,
            'file_rows': None,

            'cleaning_delta_time': None,
            'cleaning_rows': None,

            'spatial_join_delta_time': None,
            'spatial_join_rows': None,

            'trajectory_delta_time': None,
            'trajectory_rows': None,

            'cell_construct_delta_time': None,
            'cell_construct_rows': None,

            'bulk_insert_delta_time': None,
            'bulk_insert_rows': None,

            'total_delta_time': None,
        }

        self._log_settings = {
            'log_etl_stage_time': True,
            'log_etl_stage_rows': True,
            'log_file_rows': True,
            'log_requirements': True,
        }

    def log_etl_stage_time(self, stage_name: str, stage_start_time=None, stage_end_time=None):
        """Log the time and number of rows of a given ETL stage.

        Keyword arguments:
            stage_name: name of the ETL stage, must be one of the following:
                'cleaning', 'spatial_join', 'trajectory', 'cell_construct', 'bulk_insert'
            stage_start_time: start time of the ETL stage
            stage_end_time: end time of the ETL stage
        """
        self._valid_stage_name(stage_name)

        if self._log_settings['log_etl_stage_time']:
            if isinstance(stage_start_time and stage_end_time, datetime):
                time_delta = timedelta.total_seconds(stage_end_time - stage_start_time)
            else:
                time_delta = stage_end_time - stage_start_time

            self.log_dict[stage_name + '_delta_time'] = time_delta
            self._log_total_delta_time()

    def _log_total_delta_time(self):
        """Calculate the total time of the ETL process."""
        suffix = '_delta_time'
        self.log_dict['total_delta_time'] = sum([self.log_dict[key] for key in self.log_dict
                                                 if key.endswith(suffix)
                                                 and not key.startswith('total')
                                                 and self.log_dict[key] is not None])

    def log_etl_stage_rows_df(self, stage_name: str, stage_df: Union[pd.DataFrame, gpd.GeoDataFrame]):
        """Log the number of rows of a given ETL stage.

        Keyword arguments:
            stage_name: name of the ETL stage, must be one of the following:
                'cleaning', 'spatial_join', 'trajectory', 'cell_construct', 'bulk_insert'
            stage_df: dataframe for the ETL stage
        """
        self._valid_stage_name(stage_name)

        if self._log_settings['log_etl_stage_rows']:
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
        self._valid_stage_name(stage_name)
        if self._log_settings['log_etl_stage_rows']:
            try:
                if self.log_dict[stage_name + '_rows'] is None:
                    self.log_dict[stage_name + '_rows'] = cursor.rowcount
                else:
                    self.log_dict[stage_name + '_rows'] += cursor.rowcount  # To account for bulk insert which needs
            except AttributeError:
                raise AttributeError(f'Invalid cursor object: {type(cursor)}')

    def _valid_stage_name(self, stage_name):
        """Check if the stage name is valid."""
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
        if self._log_settings['log_file_rows']:
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
        """
        requirements = []
        if self._log_settings['log_requirements']:
            for line in open(requirements_path, 'r'):
                if line.startswith('#'):
                    continue
                requirements.append(line.strip())
        self.log_dict['requirements'] = requirements

    def configure_log_settings(self, log_etl_stage_time=True, log_etl_stage_rows=True,
                               log_file_rows=True, log_requirements=True):
        """Configure the log settings, in case the user wants to only log some information.

        Keyword arguments:
            log_etl_stage_time: if True, logs the delta time of each ETL stage
            log_etl_stage_rows: if True, logs the number of rows for each ETL stage
            log_file_rows: if True, logs the name, size and number of rows of files
            log_requirements: if True, logs the requirements of the ETL process
        """
        self._log_settings['log_etl_stage_time'] = log_etl_stage_time
        self._log_settings['log_etl_stage_rows'] = log_etl_stage_rows
        self._log_settings['log_file_rows'] = log_file_rows
        self._log_settings['log_requirements'] = log_requirements

    def configure_log_false(self):
        """Configure the log settings to False, in case the user wants to log nothing."""
        self._log_settings['log_etl_stage_time'] = False
        self._log_settings['log_etl_stage_rows'] = False
        self._log_settings['log_file_rows'] = False
        self._log_settings['log_requirements'] = False

    def reset_log(self):
        """Reset the log dictionary."""
        for key in self.log_dict:
            self.log_dict[key] = None
        now = datetime.now()
        self.log_dict['import_date'] = now.date()
        self.log_dict['import_time'] = now.time()
        self.log_dict['requirements'] = []

    def get_logs_db(self):
        """Return a pandas DataFrame containing the logs."""
        df = pd.DataFrame.from_dict(self.log_dict, orient='index').T
        return df

    def get_logs_dict(self):
        """Return a dictionary containing the logs."""
        return self.log_dict
