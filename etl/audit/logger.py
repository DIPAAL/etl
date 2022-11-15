from datetime import datetime, timedelta
from etl.insert.bulk_inserter import BulkInserter

import os

class AuditLogger():

    def __init__(self):
        self.dict_process = {}
        self.dict_version = {}
        self.dict_file = {}
        self._log_settings = {
            'log_proces_time': True,
            'log_proces_rows': True,
            'log_file': True
        }

    def get_logs(self):
        return {
            'version_logs': self.dict_version,
            'process_logs': self.dict_process,
            'file_logs': self.dict_file
        }
    def log_process(self, process_name, process_start_time = None, process_end_time = None, process_rows = None):
        if not self._log_settings['log_proces_time'] or self._log_settings['log_proces_rows']:
            return 0 # No logging

        self.dict_process[process_name] = {}

        if self._log_settings['log_proces_time']:
            if isinstance(process_start_time and process_end_time, datetime):
                self.dict_process[process_name]['process_timespan'] = timedelta.total_seconds(process_end_time - process_start_time)
            else:
                self.dict_process[process_name]['process_timespan'] = process_end_time - process_start_time

        if self._log_settings['log_proces_rows']:
            self.dict_process[process_name]['process_rows'] = process_rows

    def log_version(self, dict_version):
        for key in dict_version:
            self.dict_version[key] = self._get_version_values(dict_version[key])

    def _get_version_values(self, version_name):
        numbers = [int(s) for s in str.split(version_name) if s.isdigit()]
        return dict(zip(['minor', 'middle', 'major'], numbers))

    def log_file(self, file_path):
        if self._log_settings['log_file']:
            file_name = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)
            file_rows = self._get_file_rows(file_path)
            self.dict_file[file_name] = {
                'file_path': file_path,
                'file_size': file_size,
                'file_rows': file_rows
            }

    @staticmethod
    def _get_file_rows(file_path):
        with open(file_path, 'r') as f:
            for count, lines in enumerate(f):
                pass
        return count + 1

    def config_log_settings(self, log_versions : bool, log_proces_time : bool, log_proces_rows : bool, log_file : bool):
        self._log_settings['log_versions'] = log_versions
        self._log_settings['log_proces_time'] = log_proces_time
        self._log_settings['log_proces_rows'] = log_proces_rows
        self._log_settings['log_file'] = log_file



