# realtime-anomaly-detector

The realtime anomaly detector constructs per-minute time series from clickhouse, and detects anomalies based on Ray
framework (using Ray core API).

The input data source is masked dataset from our customers. It contains multi-dimensional data with different metrics
and dimensions across customers. The anomaly is detected on a user configured set of metrics, and each metrics has its
own time series. Each anomaly object is associated with a user configured set of dimensions, which are used by the
group-by aggregation queries in clickhouse when we construct time series.

Ideally each customer's anomaly detection progress should be independent, because one customer's processing delay
shouldn't interfere with other customers. We want to achieve this customer level isolation.

The computation model between Ray and Spark is different.

For Ray, we mostly rely on Ray core API, and each Ray actor has its own internal timing, queries from clickhouse,
computes time series, maintains algorithm state, detects anomalies, and persists anomalies into clickhouse. The overall
computation is asynchronous, as each Ray actor is independent, one per customer.

For Spark, the computation at each stage mentioned above is synchronized, because RDD (or dataframe) needs to ensure
all partitions have finished computation before moving to the next stage. Therefore, it cannot provide good customer
level isolation.


## Ray

Github: https://github.com/ray-project/ray

Quick start of Ray: https://docs.ray.io/en/latest/ray-overview/getting-started.html#ray-core-quickstart

## Spark

Github: https://github.com/apache/spark

Quick start: https://spark.apache.org/docs/latest/api/python/index.html

## Usage

python main.py --mode=ray
