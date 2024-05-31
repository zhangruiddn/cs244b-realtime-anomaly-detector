
import unittest
import json
import pandas as pd

from src.anomaly_detector import AnomalyDetector


class TestAnomalyDetector(unittest.TestCase):

    def setUp(self):
        # Several spikes have been added:
        #   iphone_spike = 15 <= minute < 25 or 35 <= minute < 45
        #   pc_spike = 25 <= minute < 40
        with open('data/anomaly_test_data.json', 'r', encoding='utf-8') as file:
            self.test_data = json.load(file)

        config = {'decay_factor': 0.8, 'health_band_threshold': 2,
                  'anomaly_tolerance_threshold': 5, 'warm_up_minute_threshold': 10,
                  'anomaly_end_minute_threshold': 2}
        experience_metrics = ['page_load_time', 'checkout_conversion_rate']
        self.detector = AnomalyDetector(config, experience_metrics)

    def test_detect_anomalies(self):
        for timestamp, records in self.test_data.items():
            df = pd.DataFrame(records)  # The json array is converted to dataframe rows automatically.
            anomalies = self.detector.detect_anomalies({int(timestamp): {'deviceName': df}})
            print(f"minute={timestamp}, anomalies={str(anomalies)}")


if __name__ == '__main__':
    unittest.main()

