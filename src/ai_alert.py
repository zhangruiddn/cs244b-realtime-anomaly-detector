import configparser
import ray
import time
from src.customer_data_processor import CustomerDataProcessor
from src.util import parse_int_array_config

# TODO: add more web APIs
class AIAlert:
    def __init__(self, config_path, mode='ray'):
        config = configparser.ConfigParser()
        config.read(config_path)

        ch_config = {
            'host': config.get('clickhouse', 'host'),
            'port': config.getint('clickhouse', 'port'),
            'username': config.get('clickhouse', 'username'),
            'password': config.get('clickhouse', 'password'),
            'database': config.get('clickhouse', 'database'),
            'kafka_broker': config.get('clickhouse', 'bootstrap_servers'),
            'data_delay_minute': config.get('ai-alert', 'data_delay_minute'),
            'session_summary_table': config.get('ai-alert', 'session_summary_table')
        }

        self.task_schedule_interval_sec = int(config.get('ai-alert', 'task_schedule_interval_sec'))
        customer_ids = parse_int_array_config(config.get('ai-alert', 'customer_ids'))

        # There are two modes: 'ray' and 'local' mode.
        self.mode = mode
        if self.mode == 'ray':
            ray.init()
            # Creates a Ray actor for each customer. We distribute load by customerId.
            self.customer_processors = [
                CustomerDataProcessor.remote(customer_id, ch_config, 0) for customer_id in customer_ids
            ]
        else:
            self.customer_processors = [
                CustomerDataProcessor(customer_id, ch_config, 0) for customer_id in customer_ids
            ]

    def run(self):
        if self.mode == 'ray':
            self.run_in_ray_mode()
        else:
            self.run_in_local_mode()

    def run_in_ray_mode(self):
        while True:
            print("Running AI Alert in Ray mode...")

            # Dispatches tasks for each customer in the latest data. The task is evoked asynchronously.
            # The header node only keeps track of execution status. The algorithm state is kept in the worker nodes.
            futures = [customer_processor.run.remote() for customer_processor in self.customer_processors]

            # Waits for the tasks to complete, or timeout
            done_ids, not_done_ids = ray.wait(
                futures,
                num_returns=len(futures),
                timeout=self.task_schedule_interval_sec - 5
            )

            # Checks if all tasks finished in time
            if not_done_ids:
                print(f"Some tasks did not finish in time: {not_done_ids}")
                # This is probably ok for one time; next round the delayed task can handle 2 minutes of data.
                # However, we need to be careful about continuously increased delays.

            # Waits for the next interval
            time.sleep(self.task_schedule_interval_sec)

    def run_in_local_mode(self):
        print("Running AI Alert in local mode...")

        for processor in self.customer_processors:
            processor.run()
