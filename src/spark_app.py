from pyspark.sql import SparkSession

# Initializes Spark Session
spark = SparkSession.builder \
    .appName("RealtimeAnomalyDetector") \
    .config("spark.jars", "/Users/rzhang/clickhouse-jdbc-0.4.6-all.jar") \
    .getOrCreate()

clickhouse_url = "jdbc:clickhouse://clickhouse1.us-east4.qe.gcp.conviva.com:8123/default"
clickhouse_properties = {
    "user": "default",
    "password": ""
}

# Loads data into a DataFrame
query = """
SELECT customerId, concatWithSeparator('__AND__', deviceName), avg(lifeSessionPageLoadDurationMs), sum(lifeSessionEventCount), count(*) AS group_size
FROM app_sessionlet_realtime_dist
WHERE toStartOfMinute(intvStartTimeMs) = toDateTime(1717183620)
GROUP BY customerId, deviceName
HAVING group_size >= 50
ORDER BY customerId, deviceName
"""
df = spark.read.jdbc(url=clickhouse_url, table=f"({query}) as t", properties=clickhouse_properties)

from pyspark.sql.functions import col, avg, sum, window

# Compute aggregated metrics per minute
aggregated_df = df.groupBy("customerId", window("timestamp", "1 minute")) \
    .agg(avg("lifeSessionPageLoadDurationMs").alias("average_life_session_page_load_time"), avg("lifeSessionEventCount").alias("average_life_session_event_count"))

# TODO: plug in the custom transformation logic using the implemented anomaly detection algorithm in customer_data_processor
