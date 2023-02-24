"""Module responsible for logging details about the execution of each stage of the ETL process."""
from datetime import datetime

import os
import pandas as pd

DEBUG_KEY = 'debug_info'
ROWS_KEY = 'rows'
TIMINGS_KEY = 'timings'


class AuditLogger:
    """Class responsible for logging details about the execution of each stage of the ETL process.

    Methods:
        log_file(file_path): log the name, size and number of rows for a given file
        log_etl_version(): log the version of the ETL process
        log_requirements(requirements_path): log the requirements of the ETL process
        reset_logs(): reset the logs
        to_df(): return a dataframe containing the logs
        get_logs_dict(): return the dictionary containing the logs

    Attributes:
        log_dict (dict): dictionary containing the logs
    """

    def __init__(self):
        """Construct an instance of the AuditLogger class."""
        self.log_dict = None
        self.reset_log()

    def _log_etl_version(self):
        """Log the version of the ETL process."""
        self.log_dict['etl_version'] = os.getenv('tag', 'local_dev')

    def log_file(self, file_path):
        """Log the file name, size and number of rows.

        Keyword arguments:
            file_path: path to the file
        """
        self.log_dict['file_name'] = os.path.basename(file_path)
        self.log_dict['file_size'] = os.path.getsize(file_path)
        # Do not attempt to count the rows of a pickle because it is binary
        if not file_path.endswith('.pkl'):
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

    def _log_requirements(self, requirements_path='requirements.txt'):
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

    def reset_log(self):
        """Reset the log dictionary."""
        self.log_dict = {
            'import_datetime': datetime.now(),
            'debug_info': {
                TIMINGS_KEY: {},
                ROWS_KEY: {},
            },
        }
        self._log_requirements()
        self._log_etl_version()

    def to_dataframe(self):
        """Return a pandas DataFrame containing the logs."""
        self.log_dict['total_delta_time'] = int((datetime.now() - self.log_dict['import_datetime']).total_seconds())
        df = pd.DataFrame.from_dict(self.log_dict, orient='index').T
        return df

    def get_logs_dict(self):
        """Return a dictionary containing the logs."""
        return self.log_dict


# Global audit logger class object, for storing logs.
global_audit_logger = AuditLogger()
