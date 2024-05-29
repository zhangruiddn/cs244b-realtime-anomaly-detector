import ray
import pickle
from src.anomaly_detector import AnomalyDetector
from src.root_cause_miner import RootCauseMiner

@ray.remote
class CustomerDataProcessor:
    def __init__(self, customer_id, ch_client, shift_sec):
        self.customer_id = customer_id
        self.ch_client = ch_client
        self.shift_sec = shift_sec  # TODO: used to distribute query load in clickhouse
        self.algo_state = 0  # TODO: check what algorithm state is check-pointed in Spark today
        self.load_state()
        self.anomaly_detector = AnomalyDetector()
        self.root_cause_miner = RootCauseMiner()

    def run(self):
        # Fetches the latest minute's data for the customer.
        # Because of time shifts of task schedule, occasionally we may fetch 0 or 2 minutes of data.
        latest_data = self.ch_client.fetch_latest_data(self.customer_id)
        self.update_state(latest_data)

        # Detect anomalies based on the updated algorithm state
        anomalies = self.anomaly_detector.detectAnomalies(self.algo_state)

        # Diagnose root cause based on anomalies
        experience_cohorts = self.root_cause_miner.localizeExperienceCohorts(anomalies)  # tree based search
        root_cause = self.root_cause_miner.diagnoseRootCause(experience_cohorts)  # simple aggregation query

        # Persist root cause, time series and other data into alert DB

    def update_state(self, latest_data):
        # TODO: update baseline, area, and other statistics in algo_state
        pass

    def save_state(self):
        # Serializes and saves state to clickhouse. The benefit is that the state is queryable compared with Spark.
        serialized_state = pickle.dumps(self.algo_state)  # TODO: consider flatten the state into different columns
        self.ch_client.save_state(f"customer_sum_{self.customer_id}", serialized_state)

    def load_state(self):
        # Loads and deserializes state from clickhouse
        serialized_state = self.ch_client.get(f"customer_sum_{self.customer_id}")
        if serialized_state is not None:
            self.algo_state = pickle.loads(serialized_state)

    def get_state(self):
        return self.algo_state
