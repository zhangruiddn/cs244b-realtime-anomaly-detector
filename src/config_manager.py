import tomli


# Parses configuration parameters. So far all configurations are static. In the future, this class should also manage
# dynamic config update.
class ConfigManager:
    def __init__(self, config_path: str):
        # Parses config parameters.
        with open(config_path, 'rb') as config_file:
            self.config = tomli.load(config_file)

    def get_customer_ids(self):
        return self.config['realtime-alert']['customer_ids']

    def get_service_level_config(self):
        return {
            'task_schedule_interval_sec': self.config['realtime-alert']['task_schedule_interval_sec'],
            'data_delay_minute': self.config['realtime-alert']['data_delay_minute']
        }

    def get_clickhouse_config(self):
        return {
            'host': self.config['clickhouse']['host'],
            'port': self.config['clickhouse']['port'],
            'username': self.config['clickhouse']['username'],
            'password': self.config['clickhouse']['password'],
            'database': self.config['clickhouse']['database'],
            'kafka_broker': self.config['clickhouse']['bootstrap_servers'],
            'group_size_threshold': self.config['realtime-alert']['group_size_threshold'],
            'session_summary_table': self.config['realtime-alert']['session_summary_table'],
            'experience_group_bys': self.config['realtime-alert']['experience_group_bys'],
            'experience_metrics': self.config['realtime-alert']['experience_metrics'],
            'trace_group_bys': self.config['realtime-alert']['trace_group_bys'],
            'trace_metrics': self.config['realtime-alert']['trace_metrics']
        }

    def get_anomaly_detector_config(self):
        return {
            'decay_factor': self.config['realtime-alert-anomaly-detector']['decay_factor'],
            'health_band_threshold': self.config['realtime-alert-anomaly-detector']['health_band_threshold'],
            'anomaly_tolerance_threshold': self.config['realtime-alert-anomaly-detector']['anomaly_tolerance_threshold'],
            'warm_up_minute_threshold': self.config['realtime-alert-anomaly-detector']['warm_up_minute_threshold'],
            'anomaly_end_minute_threshold': self.config['realtime-alert-anomaly-detector']['anomaly_end_minute_threshold']
        }
