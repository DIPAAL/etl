"""Module responsible for logging details about the execution of each stage of the ETL process."""
import json
from datetime import datetime

import os
import pandas as pd
import geopandas as gpd
from typing import Union
from etl.constants import ETL_STAGE_CLEAN, ETL_STAGE_SPATIAL, ETL_STAGE_TRAJECTORY, \
    ETL_STAGE_CELL, ETL_STAGE_BULK

DELTA_TIME_SUFFIX = '_delta_time'
ROWS_SUFFIX = '_rows'
STATS_SUFFIX = '_stats'


class AuditLogger:
    """Class responsible for logging details about the execution of each stage of the ETL process.

    Methods:
        log_etl_stage_time(stage_name, stage_start_time, stage_end_time): log the delta time of a given ETL stage
        log_etl_stage_rows_df(stage_name, stage_df): log the number of rows of a given ETL stage given a dataframe
        log_etl_stage_rows_cursor(stage_name, cursor): log the number of rows of a given ETL stage given a cursor
        log_bulk_insertion(sequence_name, inserted_rows): add the bulk insertion statistics to the log dictionary
            for a given sequence name
        log_file(file_path): log the name, size and number of rows for a given file
        log_etl_version(): log the version of the ETL process
        log_requirements(requirements_path): log the requirements of the ETL process
        log_row_false(): configure the logger to not log the number of rows of each ETL stage
        reset_logs(): reset the logs
        to_df(): return a dataframe containing the logs
        get_logs_dict(): return the dictionary containing the logs

    Attributes:
        log_dict (dict): dictionary containing the logs
        log_file_rows: boolean indicating if the number of rows of the input file should be logged
        log_etl_stage_rows: boolean indicating if the number of rows of each ETL stage should be logged
    """

    def __init__(self):
        """Construct an instance of the AuditLogger class."""
        self.reset_log()
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
        self._validate_stage_name(stage_name, DELTA_TIME_SUFFIX)

        time_delta = stage_end_time - stage_start_time

        self.log_dict[stage_name + DELTA_TIME_SUFFIX] = time_delta
        self._log_total_delta_time()

    def _log_total_delta_time(self):
        """Calculate the total time of the ETL process."""
        suffix = DELTA_TIME_SUFFIX
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
        self._validate_stage_name(stage_name, ROWS_SUFFIX)

        if self.log_etl_stage_rows:
            if isinstance(stage_df, (pd.DataFrame, gpd.GeoDataFrame)):
                self.log_dict[stage_name + ROWS_SUFFIX] = len(stage_df.index)
            else:
                raise TypeError(f'Invalid type for stage_df: {type(stage_df)}')

    def log_etl_stage_rows_cursor(self, stage_name: str, cursor):
        """Log the number of rows of a given ETL stage.

        Keyword arguments:
            stage_name: name of the ETL stage, must be one of the following:
                'cleaning', 'spatial_join', 'trajectory', 'cell_construct', 'bulk_insert'
            cursor: cursor object for the ETL stage
        """
        self._validate_stage_name(stage_name, ROWS_SUFFIX)
        if self.log_etl_stage_rows:
            try:
                self.log_dict[stage_name + ROWS_SUFFIX] = cursor.rowcount
            except AttributeError:
                raise AttributeError(f'Invalid cursor object: {type(cursor)}')

    def log_bulk_insertion(self, sequence_name, inserted_rows):
        """Add the bulk insertion statistics to the log dictionary for a given sequence name.

        Accumulate the number of rows inserted for each sequence name, to allow batching.

        Keyword arguments:
            sequence_name: name of the sequence
            inserted_rows: number of rows inserted
        """
        if self.log_dict[f'{ETL_STAGE_BULK}{STATS_SUFFIX}'].get(sequence_name) is None:
            self.log_dict[f'{ETL_STAGE_BULK}{STATS_SUFFIX}'][sequence_name] = inserted_rows
            return

        self.log_dict[f'{ETL_STAGE_BULK}{STATS_SUFFIX}'][sequence_name] += inserted_rows

    def _validate_stage_name(self, stage_name, suffix):
        """Check if the stage name is valid.

        Keyword arguments:
            stage_name: name of the ETL stage, must be one of the following:
                'cleaning', 'spatial_join', 'trajectory', 'cell_construct', 'bulk_insert'
            suffix: suffix to be appended to the stage name, representing the type of log, must be one of the following:
                '_delta_time', '_rows', '_stats'
        """
        if stage_name + suffix not in self.log_dict:
            raise ValueError(f'Invalid name for ETL stage: {stage_name}')

    def log_etl_version(self):
        """Log the version of the ETL process."""
        self.log_dict['etl_version'] = os.getenv('tag', 'local_dev')

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

        Should only be used if performance is an issue.
        """
        self.log_file_rows = False
        self.log_etl_stage_rows = False

    def reset_log(self):
        """Reset the log dictionary."""
        self.log_dict = {
            'import_datetime': datetime.now(),
            'requirements': [],
            'etl_version': None,

            'file_name': None,
            'file_size': None,
            'file_rows': None,

            f'{ETL_STAGE_CLEAN}{DELTA_TIME_SUFFIX}': None,
            f'{ETL_STAGE_CLEAN}{ROWS_SUFFIX}': None,

            f'{ETL_STAGE_SPATIAL}{DELTA_TIME_SUFFIX}': None,
            f'{ETL_STAGE_SPATIAL}{ROWS_SUFFIX}': None,

            f'{ETL_STAGE_TRAJECTORY}{DELTA_TIME_SUFFIX}': None,
            f'{ETL_STAGE_TRAJECTORY}{ROWS_SUFFIX}': None,

            f'{ETL_STAGE_CELL}{DELTA_TIME_SUFFIX}': None,
            f'{ETL_STAGE_CELL}{ROWS_SUFFIX}': None,

            f'{ETL_STAGE_BULK}{DELTA_TIME_SUFFIX}': None,
            f'{ETL_STAGE_BULK}{STATS_SUFFIX}': {},

            'total_delta_time': None,
        }

        self.log_etl_version()

    def to_dataframe(self):
        """Return a pandas DataFrame containing the logs."""
        df = pd.DataFrame.from_dict(self.log_dict, orient='index').T
        df[ETL_STAGE_BULK + STATS_SUFFIX] = df[ETL_STAGE_BULK + STATS_SUFFIX].apply(lambda x: json.dumps(x))
        return df

    def get_logs_dict(self):
        """Return a dictionary containing the logs."""
        return self.log_dict


# Global audit logger class object, for storing logs.
global_audit_logger = AuditLogger()
