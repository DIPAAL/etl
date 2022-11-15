from datetime import datetime, timedelta
from etl.insert.bulk_inserter import BulkInserter

import os

class AuditLogger():

    def __init__(self):
        self.log_dict = {
            'import_date': datetime.now(),
            'requirements': {},
            'etl_version' : None,

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
            'log_file': True,
            'log_requirements': True,
        }

    def get_logs(self):
        return self.log_dict

    def log_etl_stage(self, stage_name, stage_start_time = None, stage_end_time = None, stage_rows = None):
        # TODO: Handle key error so it it's clear that the stage name is not valid
        if self._log_settings['log_etl_stage_time']:
            if isinstance(stage_start_time and stage_end_time, datetime):
                time_delta = timedelta.total_seconds(stage_end_time - stage_start_time)
            else:
                time_delta = stage_end_time - stage_start_time

            self.log_dict[stage_name + '_delta_time'] = time_delta

        if self._log_settings['log_etl_stage_rows']:
            self.log_dict[stage_name + '_rows'] = stage_rows

    def log_total_delta_time(self):
        suffix = '_delta_time'
        self.log_dict['total_delta_time'] = sum([self.log_dict[key] for key in self.log_dict if key.endswith(suffix)
                                                 and not key.startswith('total')])

    def log_etl_version(self, etl_version):
        self.log_dict['etl_version'] = etl_version

    def log_file(self, file_path):
        if self._log_settings['log_file']:
            self.log_dict['file_name'] = os.path.basename(file_path)
            self.log_dict['file_size'] = os.path.getsize(file_path)
            self.log_dict['file_rows'] = self._get_file_rows(file_path)

    @staticmethod
    def _get_file_rows(file_path):
        with open(file_path, 'r') as f:
            for count, lines in enumerate(f):
                pass
        return count + 1

    def log_requirements(self, requirements_path='requirements.txt'):
        if self._log_settings['log_requirements']:
            for line in open(requirements_path):
                if line.startswith('#'):
                    continue
                package, version = line.split('==')
                self.log_dict['requirements'][package] = version

    def config_log_settings(self, log_etl_stage_time = True, log_etl_stage_rows = True,
                                  log_file = True, log_requirements = True):
        self._log_settings['log_etl_stage_time'] = log_etl_stage_time
        self._log_settings['log_etl_stage_rows'] = log_etl_stage_rows
        self._log_settings['log_file'] = log_file
        self._log_settings['log_requirements'] = log_requirements

    def config_log_false(self):
        self._log_settings['log_etl_stage_time'] = False
        self._log_settings['log_etl_stage_rows'] = False
        self._log_settings['log_file'] = False
        self._log_settings['log_requirements'] = False

    def reset_log(self):
        for key in self.log_dict:
            self.log_dict[key] = None
        self.log_dict['import_date'] = datetime.now()
        self.log_dict['requirements'] = {}

