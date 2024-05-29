import configparser
import ray
import time
from src.clickhouse_client import ClickHouseClient
from src.customer_data_processor import CustomerDataProcessor

# TODO: add more web APIs
class AIAlert:
    def __init__(self):

        config = configparser.ConfigParser()
        config.read('config.ini')

        self.ch_client = ClickHouseClient(
            host=config.get('clickhouse', 'host'),
            port=config.getint('clickhouse', 'port'),
            username=config.get('clickhouse', 'username'),
            password=config.get('clickhouse', 'password'),
            database=config.get('clickhouse', 'database'),
            kafka_broker=config.get('clickhouse', 'bootstrap_servers')
        )

        self.task_schedule_interval_sec = config.get('ai-alert', 'task_schedule_interval_sec')

        ray.init()

        # Creates a Ray actor for each customer. We distribute load by customerId
        customer_ids = config.get('ai-alert', 'customer_ids')
        self.customer_processors = {customer_id: CustomerDataProcessor.remote(customer_id) for customer_id in customer_ids}

    def run(self):
        while True:
            # Dispatches tasks for each customer in the latest data. The task is evoked asynchronously.
            # The header node only keeps track of execution status. The algorithm state is kept in the worker nodes.
            futures = [customer_processor.run.remote() for customer_processor in self.customer_processors]

            # Waits for the tasks to complete, or timeout
            done_ids, not_done_ids = ray.wait(futures, num_returns=len(futures), timeout=self.task_schedule_interval_sec - 5)

            # Checks if all tasks finished in time
            if not_done_ids:
                print(f"Some tasks did not finish in time: {not_done_ids}")
                # This is probably ok for one time; next round the delayed task can handle 2 minutes of data.
                # However, we need to be careful about continuously increased delays.

            # Waits for the next interval
            time.sleep(self.task_schedule_interval_sec)
