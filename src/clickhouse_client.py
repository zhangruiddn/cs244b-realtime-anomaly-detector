from typing import List, Dict, Union

import clickhouse_connect
import pandas as pd
from clickhouse_connect.driver.query import QueryResult

from src.util import parse_group_by_combination_config


DIMENSION_COMBINE_STR = "__AND__"

# The python clickhouse client, responsible to construct and run queries. It's stateless.
class ClickHouseClient:
    def __init__(self, customer_id: int, ch_config):
        self.client = clickhouse_connect.get_client(
            host=ch_config['host'],
            port=ch_config['port'],
            username=ch_config['username'],
            password=ch_config['password'],
            database=ch_config['database']
        )
        self.kafka_broker: str = ch_config['kafka_broker']
        self.database: str = ch_config['database']
        self.customer_id: int = customer_id
        self.group_size_threshold: int = ch_config['group_size_threshold']
        self.session_summary_table: str = ch_config['session_summary_table']
        self.experience_group_bys: List[str] = ch_config['experience_group_bys']
        self.experience_metrics: List[str] = ch_config['experience_metrics']
        self.trace_group_bys: List[str] = ch_config['trace_group_bys']
        self.trace_metrics: List[str] = ch_config['trace_metrics']

    def execute_query(self, query: str) -> QueryResult:
        return self.client.query(query)

    # Returns the latest minute available for the customer in clickhouse.
    def get_latest_minute(self) -> int:
        latest_minute_query = f"""
            SELECT toUnixTimestamp(max(toStartOfMinute(intvStartTimeMs))) as latest_minute
            FROM {self.session_summary_table}
            WHERE customerId = {self.customer_id}
        """
        print(f"latest_minute_query={latest_minute_query}")
        latest_minute_result = self.execute_query(latest_minute_query)

        if not latest_minute_result:
            return -1  # no data available
        return latest_minute_result.result_set[0][0]  # as unix epoch timestamp

    # Aggregates the experience metrics for each group-by dimension configured.
    # Returns a dictionary where each group-by config serves as a key, and the corresponding value is a DataFrame
    # containing aggregated metrics. In the DataFrame, the first column is the group-by dimension, the last column is
    # the group_size, and the in-between columns are all metric columns.
    #
    # So far the queries are executed in sync. If clickhouse is under-utilized, we can create multiple threads to send
    # queries in parallel, each query sent by a different python client (because it's not thread safe).
    #
    # Example aggregation query:
    #
    def fetch_experience_data_for_minute(self, minute_timestamp: int) -> Dict[str, pd.DataFrame]:
        result_dict = {}
        for group_by in self.experience_group_bys:
            group_by_clause = ", ".join(parse_group_by_combination_config(group_by))
            select_clause = (", ".join([f"concatWithSeparator('{DIMENSION_COMBINE_STR}', {group_by_clause})"] +
                                       self.experience_metrics))

            aggregation_query = f"""
                SELECT {select_clause}, count(*) AS group_size
                FROM {self.session_summary_table}
                WHERE customerId = {self.customer_id}
                    AND toStartOfMinute(intvStartTimeMs) = toDateTime({minute_timestamp})
                GROUP BY {group_by_clause}
                HAVING group_size >= {self.group_size_threshold}
                ORDER BY {group_by_clause}
            """
            print(f"aggregation_query={aggregation_query}")
            result = self.execute_query(aggregation_query)
            print(f"group_by={group_by}, result_size={len(result.result_set)}")

            # Converts the clickhouse query result to a pandas DataFrame
            df = pd.DataFrame(result.result_set, columns=[group_by] + self.experience_metrics + ["group_size"])
            result_dict[group_by] = df

        return result_dict

    def save_state(self, serialized_state):
        pass

    def load_state(self):
        pass
