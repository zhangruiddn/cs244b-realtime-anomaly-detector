import math
from dataclasses import dataclass
from typing import Dict, List

import pandas as pd


# Stores the trending metric state per group metric. When new data comes in, the metric state will be updated.
# Anomaly is defined at metric level (per group per metric).
@dataclass
class MetricState:
    mean: float = 0.0  # trending mean of the metric
    variance: float = 0.0  # trending variance of the metric
    accumulated_diff: float = 0.0  # accumulated difference between the metric value and the bound of its health band
    anomaly_detected: bool = False  # whether an anomaly has been detected for the metric
    anomaly_end_minute: int = 0  # number of minutes that the anomaly metric becomes and stays within the health band.
                                 # This is used to end an anomaly. It's only valid when anomaly_detected is True.


# Stores the group level state. The group identifier is the dimension value (e.g. iPhone, chrome) or dimension value
# combination of the group (e.g. iPhone AND chrome). Each group has multiple metrics, each with a MetricState.
@dataclass
class GroupState:
    metrics: Dict[str, MetricState]  # metric name to metric state
    group_size: int = 0  # the session count of the group
    warm_up_minute: int = 0  # number of minutes that the group has received data, may not be continuously


# Stores anomaly information to be persisted into clickhouse. The anomaly info is per metric per group.
@dataclass
class AnomalyInfo:
    metric_value: float = 0.0  # current metric value
    mean: float = 0.0  # trending mean of the metric
    std_dev: float = 0.0  # standard deviation of the metric
    accumulated_diff: float = 0.0  # accumulated difference between the metric value and the bound of its health band
    health_band: float = 0.0  # the health bandwidth above and below the trending mean
    group_size: int = 0  # the session count of the group
    # anomaly_duration_minute: int  # number of minutes since anomaly was detected


# The algorithm of anomaly detection is simple:
# 1) For each metric in a group, we keep track of the trending mean and variance, and computes a health band for it.
# 2) When the metric value exceeds (or falls below) the health band, we compute the accumulated difference between the
#    metric value and the bound of health band. An anomaly is detected if the accumulated difference exceeds a certain
#    threshold, which is customer specific based on their alert sensitivity setting.
# 3) When the metric falls within the health band for a continuous amount of time, the anomaly ends.
#
# Trending mean and variance update formula:
#   new_mean = decay_factor * old_mean + (1 - decay_factor) * new_data_point
#   new_variance = decay_factor * old_variance + (1 - decay_factor) * (new_data_point - new_mean) ** 2
# The trending mean and variance are only updated when there is no anomaly detected for the metric.
class AnomalyDetector:
    def __init__(self, config, experience_metrics: List[str]):
        self.decay_factor: float = config['decay_factor']
        self.health_band_threshold: int = config['health_band_threshold']
        self.anomaly_tolerance_threshold: int = config['anomaly_tolerance_threshold']
        self.warm_up_minute_threshold: int = config['warm_up_minute_threshold']
        self.anomaly_end_minute_threshold: int = config['anomaly_end_minute_threshold']
        self.experience_metrics: List[str] = experience_metrics
        self.trending_state: Dict[str, Dict[str, GroupState]] = {}

    # Runs the anomaly detection algorithm. Returns the anomaly groups identified.
    # Example input
    # experience_group_metrics = {
    #     1609459200: {"deviceName": df1, "browserName": df2},
    #     ...
    # }, where
    # df1 = {
    #         deviceName    m1    m2    m3    group_size
    #     0       Mac      0.1    ...   ...      275
    #     1       PC       0.2    ...   ...      811
    # }, and
    # df2 = {
    #        browserName    m1    m2    m3    group_size
    #     0      Edge      0.3    ...   ...     137
    #     1      Firefox   1.1    ...   ...     155
    #     2      Chrome    0.9    ...   ...     782
    # }
    # The caller guarantees the input is sorted by the timestamp (top level key). It also guarantees that
    #   1) the first column stores group by dimension value;
    #   2) the last column stores the group size;
    #   3) the metrics columns are stored in between the first and the last column.
    def detect_anomalies(self, experience_group_metrics: Dict[int, Dict[str, pd.DataFrame]]
                         ) -> Dict[str, Dict[str, AnomalyInfo]]:

        # Stores the anomaly info per metric per group. Top level key: metric name. Inner level key: group-by dimension
        anomaly_groups: Dict[str, Dict[str, AnomalyInfo]] = {}

        # Iterates over each minute and group
        for minute, groups in experience_group_metrics.items():
            for group_dimension_name, metrics_df in groups.items():

                # Calculates the decayed average as the trending mean
                if group_dimension_name not in self.trending_state:
                    self.trending_state[group_dimension_name] = {}

                for _, df_row in metrics_df.iterrows():
                    dimension_value = df_row.iloc[0]  # input row's first column is the group dimension value

                    # Checks if the dimension already has a state, else initializes one
                    if dimension_value not in self.trending_state[group_dimension_name]:
                        group_state = GroupState(
                            metrics={},
                            group_size=df_row.iloc[-1],
                            warm_up_minute=1,
                        )

                        # Initializes metric state for each metric
                        for metric_name in self.experience_metrics:
                            # Sets the mean with the latest metric value
                            # TODO: evaluate the initial value assignment (eg. too small variance starting with zero?)
                            group_state.metrics[metric_name] = MetricState(mean=df_row[metric_name])

                        self.trending_state[group_dimension_name][dimension_value] = group_state
                        # print(f"{dimension_value}: {str(group_state)}")
                        continue

                    # Updates group state
                    group_state = self.trending_state[group_dimension_name][dimension_value]
                    self.update_group_state(group_state, dimension_value, df_row, anomaly_groups)
                    # print(f"{dimension_value}: {str(group_state)}")

                    # TODO: consider criteria to delete a group, or not delete at all

        return anomaly_groups

    def update_group_state(self, group_state, dimension_value, df_row, anomaly_groups):
        # Updates group level state
        group_state.group_size = df_row.iloc[-1]  # input row's last column is the group size
        group_state.warm_up_minute += 1

        # Updates metric level state by iterating all metrics and detecting anomalies
        for metric_name in self.experience_metrics:
            new_value = df_row[metric_name]
            metric_state = group_state.metrics[metric_name]

            # Computes the accumulated difference between the metric value and the health band. It will be reset if the
            # metric value becomes within the health band. The health band is only valid after warm up has finished.
            std_dev = math.sqrt(metric_state.variance)
            if group_state.warm_up_minute > self.warm_up_minute_threshold:
                health_band = self.health_band_threshold * std_dev
                accumulated_diff = 0.0
                if abs(new_value - metric_state.mean) > health_band:
                    accumulated_diff = (metric_state.accumulated_diff +
                                        abs(new_value - metric_state.mean) - health_band)
                # The accumulated diff is updated regardless of whether anomaly is detected or not
                # TODO: add severity based on this
                metric_state.accumulated_diff = accumulated_diff

            # Checks whether an anomaly is detected. An anomaly is detected when the accumulated difference between the
            # metric value and the health band exceeds the given threshold.
            if (not metric_state.anomaly_detected and
                    group_state.warm_up_minute > self.warm_up_minute_threshold and
                    accumulated_diff > self.anomaly_tolerance_threshold * std_dev):
                metric_state.anomaly_detected = True  # marks anomaly
                anomaly_groups.setdefault(metric_name, {})[dimension_value] = AnomalyInfo(
                    metric_value=new_value,
                    mean=metric_state.mean,
                    std_dev=math.sqrt(metric_state.variance),
                    accumulated_diff=accumulated_diff,
                    health_band=health_band,
                    group_size=group_state.group_size
                )
                continue

            # Checks whether an anomaly has ended. An anomaly ends when the metric value stays within the health band
            # for a continuous amount of time.
            if metric_state.anomaly_detected:
                if abs(new_value - metric_state.mean) < health_band:
                    metric_state.anomaly_end_minute += 1
                    if metric_state.anomaly_end_minute > self.anomaly_end_minute_threshold:  # anomaly ends
                        metric_state.anomaly_detected = False
                        metric_state.accumulated_diff = 0.0
                        metric_state.anomaly_end_minute = 0
                else:
                    metric_state.anomaly_end_minute = 0  # resets when the metric exceeds the health band

                # Always reports anomalies during the incident period.
                anomaly_groups.setdefault(metric_name, {})[dimension_value] = AnomalyInfo(
                    metric_value=new_value,
                    mean=metric_state.mean,
                    std_dev=math.sqrt(metric_state.variance),
                    accumulated_diff=accumulated_diff,
                    health_band=health_band,
                    group_size=group_state.group_size
                )

            # Updates the trending mean and variance only if no anomaly is detected.
            if not metric_state.anomaly_detected:
                metric_state.mean = (self.decay_factor * metric_state.mean +
                                     (1 - self.decay_factor) * new_value)
                metric_state.variance = (self.decay_factor * metric_state.variance +
                                         (1 - self.decay_factor) * (new_value - metric_state.mean) ** 2)
