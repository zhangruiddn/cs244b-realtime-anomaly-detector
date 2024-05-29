import clickhouse_connect

class ClickHouseClient:
    def __init__(self, host, port, username, password, database, kafka_broker) -> None:
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database
        )
        self.kafka_broker = kafka_broker
        self.database = database

    def execute_query(self, query):
        self.client.query(query)

    def fetch_latest_data(self, customerId):
        # We haven't decided whether to store all customers' data in one table or separate tables in clickhouse.
        # Here we assume all data in one table, so we use customerId as a filter.
        # TODO: read latest minute; then minus one to fetch the data
        pass
