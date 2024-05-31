import tomllib
from typing import Dict, Union, List


# Parses configuration parameters. So far all configurations are static. In the future, this class should also manage
# dynamic config update.
class ConfigManager:
    def __init__(self, config_path: str):
        # Parses config parameters.
        with open(config_path, 'rb') as config_file:
            self.config = tomllib.load(config_file)

    def get_customer_ids(self):
        return self.config['ai-alert']['customer_ids']

    def get_service_level_config(self):
        return {
            'task_schedule_interval_sec': self.config['ai-alert']['task_schedule_interval_sec'],
            'data_delay_minute': self.config['ai-alert']['data_delay_minute']
        }

    def get_clickhouse_config(self):
        return {
            'host': self.config['clickhouse']['host'],
            'port': self.config['clickhouse']['port'],
            'username': self.config['clickhouse']['username'],
            'password': self.config['clickhouse']['password'],
            'database': self.config['clickhouse']['database'],
            'kafka_broker': self.config['clickhouse']['bootstrap_servers'],
            'group_size_threshold': self.config['ai-alert']['group_size_threshold'],
            'session_summary_table': self.config['ai-alert']['session_summary_table'],
            'experience_group_bys': self.config['ai-alert']['experience_group_bys'],
            'experience_metrics': self.config['ai-alert']['experience_metrics'],
            'trace_group_bys': self.config['ai-alert']['trace_group_bys'],
            'trace_metrics': self.config['ai-alert']['trace_metrics']
        }

    def get_anomaly_detector_config(self):
        return {
            'decay_factor': self.config['ai-alert-anomaly-detector']['decay_factor'],
            'health_band_threshold': self.config['ai-alert-anomaly-detector']['health_band_threshold'],
            'anomaly_tolerance_threshold': self.config['ai-alert-anomaly-detector']['anomaly_tolerance_threshold'],
            'warm_up_minute_threshold': self.config['ai-alert-anomaly-detector']['warm_up_minute_threshold'],
            'anomaly_end_minute_threshold': self.config['ai-alert-anomaly-detector']['anomaly_end_minute_threshold']
        }
