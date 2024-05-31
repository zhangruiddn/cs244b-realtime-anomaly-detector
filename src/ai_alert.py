
from typing import List

import ray
import time

from src.config_manager import ConfigManager
from src.customer_data_processor import CustomerDataProcessor


# The AI alert service initiates a new task every `task_schedule_interval_sec` seconds. This task involves the `run()`
# method of the customer data processor, which is responsible for generating time series data, detecting anomalies, and
# diagnosing their root causes.
#
# The service can run in two modes:
#   1) Ray mode - creates a Ray actor for each customer, distributing the load by customer_id
#   2) local mode - creates local customer data processors to process data, one for each customer_id
# Caveat: in local mode, please comment out the "@ray.remote" decoration in the customer data processor class. I haven't
# figured out an easy way to disable Ray without duplicating the class. TODO: fix this.
#
# TODO: add more web APIs
class AIAlert:
    def __init__(self, config_path: str, mode: str = 'ray'):
        # Parses config parameters.
        config_manager = ConfigManager(config_path)
        service_config = config_manager.get_service_level_config()
        ch_config = config_manager.get_clickhouse_config()
        anomaly_detector_config = config_manager.get_anomaly_detector_config()

        self.task_schedule_interval_sec: int = service_config['task_schedule_interval_sec']
        data_delay_minute: int = service_config['data_delay_minute']
        customer_ids: List[int] = config_manager.get_customer_ids()

        # There are two modes: 'ray' and 'local' mode.
        self.mode = mode
        if self.mode == 'ray':
            ray.init()
            # Creates a Ray actor for each customer. We distribute load by customerId.
            self.customer_processors = [
                CustomerDataProcessor.remote(
                    customer_id, ch_config, anomaly_detector_config, 0, data_delay_minute
                )
                for customer_id in customer_ids
            ]
        else:
            self.customer_processors = [
                CustomerDataProcessor(
                    customer_id, ch_config, anomaly_detector_config, 0, data_delay_minute
                )
                for customer_id in customer_ids
            ]

    def run(self) -> None:
        if self.mode == 'ray':
            self.run_in_ray_mode()
        else:
            self.run_in_local_mode()

    def run_in_ray_mode(self) -> None:
        while True:
            print("Running AI Alert in Ray mode...")

            # Dispatches tasks for each customer in the latest data. The task is evoked asynchronously.
            # The header node only keeps track of execution status. The algorithm state is kept in the worker nodes.
            futures = [customer_processor.run.remote() for customer_processor in self.customer_processors]

            # Waits for the tasks to complete, or timeout.
            done_ids, not_done_ids = ray.wait(
                futures,
                num_returns=len(futures),
                timeout=self.task_schedule_interval_sec - 5
            )

            # Checks if all tasks have finished in time.
            if not_done_ids:
                print(f"Some tasks did not finish in time: {not_done_ids}")
                # This is probably ok for one time; next round the delayed task can handle 2 minutes of data.
                # However, we need to be careful about continuously increased delays.

            # Waits for the next interval.
            time.sleep(self.task_schedule_interval_sec)

    def run_in_local_mode(self) -> None:
        print("Running AI Alert in local mode...")

        for processor in self.customer_processors:
            processor.run()

