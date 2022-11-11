from datetime import datetime, timedelta

class AuditLogger:

    def __init__(self):
        self.dict_process = {}
        self.dict_version = {}
        self._log_settings = {
            'log_versions': True,
            'log_proces_time': True,
            'log_proces_rows': True
        }

    def log_process(self, process_name, process_start_time, process_end_time, process_rows):
        if self._log_settings['log_proces_time']:
            if isinstance(process_start_time and process_end_time, datetime):
                self.dict_process[process_name]['process_timespan'] = timedelta.total_seconds(process_end_time - process_start_time)
            else:
                self.dict_process[process_name]['process_timespan'] = process_end_time - process_start_time,

        if self._log_settings['log_proces_rows']:
            self.dict_process[process_name]['process_rows'] = process_rows

    def log_version(self, version_name, version_value):
        if self._log_settings['log_versions']:
            self.dict_version[version_name] = version_value

    def get_version_values(self, version_name):
        return self.dict_version[version_name]  #TODO: Reimplement so it returns a list of values from string

    def config_log_settings(self, log_versions : bool, log_proces_time : bool, log_proces_rows : bool):
        self._log_settings['log_versions'] = log_versions
        self._log_settings['log_proces_time'] = log_proces_time
        self._log_settings['log_proces_rows'] = log_proces_rows