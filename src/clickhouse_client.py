import clickhouse_connect

class ClickHouseClient:
    def __init__(self, host, port, username, password, database, kafka_broker, data_delay_minute, session_summary_table):
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
	@@ -11,12 +11,43 @@ def __init__(self, host, port, username, password, database, kafka_broker) -> No
        )
        self.kafka_broker = kafka_broker
        self.database = database
        self.data_delay_minute = data_delay_minute
        self.session_summary_table = session_summary_table

    def execute_query(self, query):
        self.client.query(query)

    def get_latest_minute(self, customer_id):
        latest_minute_query = f"""
            SELECT toUnixTimestamp(max(toStartOfMinute(intvStartTimeMs))) as latest_minute
            FROM {self.session_summary_table}
            WHERE customerId = {customer_id}
        """
        print("latest_minute_query=" + latest_minute_query)
        latest_minute_result = self.execute_query(latest_minute_query)

        if not latest_minute_result:
            return -1
        return latest_minute_result[0]['latest_minute']  # as unix epoch timestamp

    def fetch_data_for_minute(self, customer_id, minute_timestamp):
        # Olap team hasn't decided whether to store all customers' data in one table or separate tables in clickhouse.
        # Here we assume all data in one table, so we use customer_id as a filter.
        data_query = f"""
            SELECT *
            FROM {self.session_summary_table}
            WHERE customerId = {customer_id}
              AND toStartOfMinute(intvStartTimeMs) = toDateTime({minute_timestamp})
        """
        print("data_query=" + data_query)
        result = self.execute_query(data_query)
        return result

    def save_state(self, customer_id, serialized_state):
        pass

    def load_state(self, customer_id):
        pass

    def get_latest_processed_minute(self):
        return self.latest_processed_minute
