from typing import Dict, Union, List

import pandas as pd
import pickle
from src.anomaly_detector import AnomalyDetector
from src.clickhouse_client import ClickHouseClient
from src.root_cause_miner import RootCauseMiner

SEC_PER_MINUTE = 60


# The customer data processor is responsible for time series construction, anomaly detection, and root cause diagnosis.
# It also manages the algorithm state, and its persistence and retrieval from alert DB. Essentially the data processor
# handles all tasks needed by AI alert at per customer level.
# In Ray mode, each processor is a Ray actor.
class CustomerDataProcessor:
    def __init__(self,
                 customer_id: int,
                 ch_config,
                 anomaly_detector_config,
                 query_shift_sec: int,
                 data_delay_minute: int):
        self.customer_id = customer_id
        self.ch_client = ClickHouseClient(customer_id, ch_config)
        self.query_shift_sec = query_shift_sec  # TODO: distributes query load in clickhouse based on this
        self.data_delay_minute = data_delay_minute
        self.algo_state = 0  # TODO: check what algorithm state is check-pointed in Spark today
        self.latest_processed_minute = -1  # epoch seconds
        self.load_state()
        self.anomaly_detector = AnomalyDetector(anomaly_detector_config, ch_config['experience_metrics'])
        self.root_cause_miner = RootCauseMiner()

    def run(self) -> None:
        # Computes time series for each experience group by config.
        experience_group_metrics = self.compute_experience_group_metrics()

        # Detects anomalies based on the latest time series.
        anomaly_groups = self.anomaly_detector.detect_anomalies(experience_group_metrics)

        # Diagnoses root causes based on anomalies detected.
        experience_cohorts = self.root_cause_miner.localize_experience_cohorts(anomaly_groups)  # recursive search
        root_cause = self.root_cause_miner.diagnose_root_cause(experience_cohorts)  # new root cause algorithm

        # Persists root causes, time series and summary data into alert DB.
        self.write_to_alert_db()

    # Returns aggregated group metrics.
    # The returned type is a nested dictionary, with outer key as the minute (in epoch seconds), and inner key as the
    # group-by dimension, and the value as the aggregated metrics for the group.
    # Example output:
    # experience_group_metrics = {
    #     1609459200: {"deviceName": df1, "browserName": df2},
    #     ...
    # }
    def compute_experience_group_metrics(self) -> Dict[int, Dict[str, pd.DataFrame]]:
        # Gets the latest minute available in clickhouse
        print(f"customer_id={self.customer_id}")
        latest_minute = self.ch_client.get_latest_minute()  # epoch seconds

        if latest_minute < 0:
            print(f"No data available for customer {self.customer_id}")
            return

        # Computes the minute to process the data.
        # This is different from latest minute. We need to ensure all TLB partitions have finished writing for the
        # minute to process. This is a temporary workaround before the data integrity service becomes available.
        minute_to_process = latest_minute - SEC_PER_MINUTE * self.data_delay_minute
        print(f"minute_to_process={minute_to_process}")

        if self.latest_processed_minute >= minute_to_process:
            print(f"Skip the minute {minute_to_process} because the latest processed minute is "
                  f"{self.latest_processed_minute}")
            return

        # Computes the aggregated metrics for each experience group in clickhouse.
        aggregated_result = {minute_to_process: self.ch_client.fetch_experience_data_for_minute(minute_to_process)}
        print(aggregated_result[minute_to_process])

        # Because of time shifts of task schedule, occasionally we may fetch multiple minutes of data.
        # TODO: add an upper limit on this dating back loop
        while self.latest_processed_minute > 0 and minute_to_process > self.latest_processed_minute:
            minute_to_process -= SEC_PER_MINUTE
            print(f"minute_to_process={minute_to_process}")
            aggregated_result[minute_to_process] = self.ch_client.fetch_experience_data_for_minute(minute_to_process)
            print(aggregated_result[minute_to_process])

        # Sorts the result based on the key (timestamp) in ascending order.
        sorted_aggregated_result = {k: aggregated_result[k] for k in sorted(aggregated_result.keys())}
        return sorted_aggregated_result

    def write_to_alert_db(self):
        # TODO: persist data in 3 tables in mariaDB
        pass

    def save_state(self):
        # Serializes and saves state to clickhouse. The benefit is that the state is queryable compared to Spark.
        serialized_state = pickle.dumps(self.algo_state)  # TODO: consider flatten the state into different columns
        self.ch_client.save_state(serialized_state)

    def load_state(self):
        # Loads and deserializes state from clickhouse
        serialized_state = self.ch_client.load_state()
        if serialized_state is not None:
            self.algo_state = pickle.loads(serialized_state)

    def get_state(self):
        return self.algo_state
