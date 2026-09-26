"""Silver: parse Bronze payloads into typed notice columns.

Reads Bronze as a stream so the job is incremental, applies the vectorised
parser, and splits the result: parseable notices go to the cleaned table,
everything else to a quarantine table. Routing failures rather than dropping
them keeps the Silver row count reconcilable against Bronze.
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from gcn_lakehouse.session import configure_session  # noqa: E402
from gcn_lakehouse.config import CatalogConfig, StreamConfig  # noqa: E402
from gcn_lakehouse.schemas import SILVER_FIELD_NAMES  # noqa: E402
from gcn_lakehouse.spark_udf import parsed_notice  # noqa: E402

from bronze_ingest import BRONZE_TABLE  # noqa: E402

SILVER_TABLE = "fermi_gbm_fin_pos_cleaned"
QUARANTINE_TABLE = "fermi_gbm_fin_pos_quarantine"


def parse_bronze(bronze: DataFrame) -> DataFrame:
    """Flatten the parsed struct beside the retained Kafka metadata."""
    parsed = bronze.withColumn("notice", parsed_notice(col("payload")))
    return parsed.select(
        *[col(f"notice.{name}").alias(name) for name in SILVER_FIELD_NAMES],
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
        col("ingested_at"),
        col("payload").alias("raw_payload"),
    )


def main() -> None:
    spark = configure_session(SparkSession.builder.getOrCreate())
    catalog, stream = CatalogConfig(), StreamConfig()

    parsed = parse_bronze(
        spark.readStream.table(catalog.table("bronze", BRONZE_TABLE))
    )

    for table, frame in (
        (SILVER_TABLE, parsed.where(col("is_parsed"))),
        (QUARANTINE_TABLE, parsed.where(~col("is_parsed"))),
    ):
        target = catalog.table("silver", table)
        (
            frame.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{stream.checkpoints}/{table}")
            .trigger(availableNow=True)
            .toTable(target)
            .awaitTermination()
        )
        print(f"Silver write complete -> {target}")


if __name__ == "__main__":
    main()
