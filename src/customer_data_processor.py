from datetime import datetime

import ray
import pickle
from src.anomaly_detector import AnomalyDetector
from src.clickhouse_client import ClickHouseClient
from src.root_cause_miner import RootCauseMiner

@ray.remote
class CustomerDataProcessor:
    def __init__(self, customer_id, ch_config, shift_sec):
        self.customer_id = customer_id
        self.ch_client = ClickHouseClient(
            host=ch_config['host'],
            port=ch_config['port'],
            username=ch_config['username'],
            password=ch_config['password'],
            database=ch_config['database'],
            kafka_broker=ch_config['kafka_broker'],
            data_delay_minute=ch_config['data_delay_minute'],
            session_summary_table=ch_config['session_summary_table']
        )
        self.shift_sec = shift_sec  # TODO: used to distribute query load in clickhouse
        self.algo_state = 0  # TODO: check what algorithm state is check-pointed in Spark today
        self.latest_processed_minute = -1
        self.load_state()
        self.anomaly_detector = AnomalyDetector()
        self.root_cause_miner = RootCauseMiner()

    def run(self):
        # Gets the latest minute available in clickhouse
        print("customer_id=" + str(self.customer_id))
        latest_minute = self.ch_client.get_latest_minute(self.customer_id)

        # Computes the latest minute to process the data. This is different from latest_minute. We need to ensure all
        # TLB partitions have finished writing for the minute.
        minute_to_process = latest_minute - datetime.timedelta(minutes=self.data_delay_minute)
        print("minute_to_process=" + minute_to_process)

        if self.latest_processed_minute >= minute_to_process:
            print(f"Skip the minute {minute_to_process} because the latest processed minute is "
                  f"{self.latest_processed_minute}")
            return

        # Computes the aggregated metrics for each group in clickhouse.
        # Because of time shifts of task schedule, occasionally we may fetch 0 or 2 minutes of the data.
        data_to_process = self.ch_client.fetch_data_for_minute(self.customer_id, minute_to_process)
        self.update_state(data_to_process)

        # Detects anomalies based on the updated algorithm state
        anomalies = self.anomaly_detector.detectAnomalies(self.algo_state)

        # Diagnoses root cause based on anomalies
        experience_cohorts = self.root_cause_miner.localizeExperienceCohorts(anomalies)  # tree based recursive search
        root_cause = self.root_cause_miner.diagnoseRootCause(experience_cohorts)  # new root cause algorithm

        # Persists root cause, time series and summary data into alert DB
        self.write_to_alert_db()

    def update_state(self, data_to_process):
        # TODO: update baseline, area, and other statistics in algo_state
        pass

    def write_to_alert_db(self):
        # TODO: persist data in 3 tables in mariaDB
        pass

    def save_state(self):
        # Serializes and saves state to clickhouse. The benefit is that the state is queryable compared to Spark.
        serialized_state = pickle.dumps(self.algo_state)  # TODO: consider flatten the state into different columns
        self.ch_client.save_state(self.customer_id, serialized_state)

    def load_state(self):
        # Loads and deserializes state from clickhouse
        serialized_state = self.ch_client.load_state(self.customer_id)
        if serialized_state is not None:
            self.algo_state = pickle.loads(serialized_state)

    def get_state(self):
        return self.algo_state
