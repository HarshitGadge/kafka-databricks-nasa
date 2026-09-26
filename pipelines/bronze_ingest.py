"""Bronze: subscribe to the GCN Kafka topic and land raw notices in Delta.

Run as a Databricks job task or notebook on a cluster with the
``spark-sql-kafka-0-10`` connector available (included in the Databricks
runtime).

Bronze deliberately performs no parsing. The payload is stored exactly as GCN
sent it, alongside the Kafka coordinates, so Silver can be rebuilt from Bronze
after a parser change without re-reading from the broker -- GCN only retains a
bounded window of history.
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from gcn_lakehouse.session import configure_session  # noqa: E402
from gcn_lakehouse.config import (  # noqa: E402
    CatalogConfig,
    SecretConfig,
    StreamConfig,
    kafka_options,
    redacted,
)

BRONZE_TABLE = "fermi_gbm_fin_pos_raw"


def read_gcn_stream(spark: SparkSession, secrets: SecretConfig, stream: StreamConfig):
    """Open the authenticated GCN subscription as a streaming DataFrame."""
    dbutils = _dbutils(spark)
    options = kafka_options(
        client_id=dbutils.secrets.get(secrets.scope, secrets.client_id_key),
        client_secret=dbutils.secrets.get(secrets.scope, secrets.client_secret_key),
        stream=stream,
    )
    print("Kafka source options:", redacted(options))
    return spark.readStream.format("kafka").options(**options).load()


def to_bronze(frame):
    """Project the Kafka record into the Bronze table shape."""
    return frame.select(
        col("value").cast("string").alias("payload"),
        col("key").cast("string").alias("message_key"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingested_at"),
    )


def _dbutils(spark: SparkSession):
    """Fetch dbutils, which is injected by the runtime rather than imported."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore

        return DBUtils(spark)
    except ImportError:  # pragma: no cover - notebook-scoped global
        return globals()["dbutils"]


def main() -> None:
    spark = configure_session(SparkSession.builder.getOrCreate())
    catalog, secrets, stream = CatalogConfig(), SecretConfig(), StreamConfig()
    target = catalog.table("bronze", BRONZE_TABLE)

    query = (
        to_bronze(read_gcn_stream(spark, secrets, stream))
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{stream.checkpoints}/{BRONZE_TABLE}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target)
    )
    query.awaitTermination()
    print(f"Bronze ingest complete -> {target}")


if __name__ == "__main__":
    main()
