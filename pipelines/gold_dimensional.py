"""Gold: snowflake dimensional model over the cleaned Silver notices.

One GCN trigger produces several notices as the localisation is refined, so the
grain of the fact table is one *notice revision* -- ``trigger_num`` plus
``record_num`` -- not one burst. Analysts wanting the final word on a burst take
the highest ``record_num`` per trigger; the ``is_latest_revision`` flag is
maintained here so they do not have to.

Dimension keys are deterministic hashes of the member attributes rather than
generated sequences. A rebuild from Bronze therefore produces identical keys,
which keeps the MERGE idempotent and the tables diffable between runs.
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from gcn_lakehouse.session import configure_session  # noqa: E402
from gcn_lakehouse.config import CatalogConfig  # noqa: E402

from silver_parse import SILVER_TABLE  # noqa: E402

FACT_TABLE = "fact_gcn_notices"
DIM_TIMING = "dim_event_timing"
DIM_SPATIAL = "dim_spatial_coords"
DIM_SUN_MOON = "dim_sun_moon_coords"


def _surrogate_key(*columns: str):
    """Deterministic key over the given columns.

    Nulls are mapped to a sentinel before hashing so that two rows differing
    only in which attribute is null do not collide on an empty digest.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("␀")) for c in columns]
    return F.sha2(F.concat_ws("␟", *parts), 256)


def build_sun_moon_dimension(silver: DataFrame) -> DataFrame:
    columns = [
        "sun_ra_deg",
        "sun_dec_deg",
        "sun_dist_deg",
        "moon_ra_deg",
        "moon_dec_deg",
        "moon_dist_deg",
        "moon_illum_pct",
    ]
    return (
        silver.select(*columns)
        .distinct()
        .withColumn("sun_moon_key", _surrogate_key(*columns))
        .select("sun_moon_key", *columns)
    )


def build_spatial_dimension(silver: DataFrame) -> DataFrame:
    """Burst position, snowflaked onto the solar/lunar context dimension."""
    columns = [
        "ra_deg",
        "dec_deg",
        "error_radius_deg",
        "galactic_lon_deg",
        "galactic_lat_deg",
        "ecliptic_lon_deg",
        "ecliptic_lat_deg",
    ]
    sun_moon_columns = [
        "sun_ra_deg",
        "sun_dec_deg",
        "sun_dist_deg",
        "moon_ra_deg",
        "moon_dec_deg",
        "moon_dist_deg",
        "moon_illum_pct",
    ]
    return (
        silver.select(*columns, *sun_moon_columns)
        .distinct()
        .withColumn("spatial_key", _surrogate_key(*columns))
        .withColumn("sun_moon_key", _surrogate_key(*sun_moon_columns))
        .select("spatial_key", "sun_moon_key", *columns)
        .dropDuplicates(["spatial_key"])
    )


def build_timing_dimension(silver: DataFrame) -> DataFrame:
    """Calendar attributes for the burst instant, for time-series rollups."""
    return (
        silver.select("burst_time", "burst_date", "notice_time")
        .distinct()
        .withColumn("timing_key", _surrogate_key("burst_time", "notice_time"))
        .withColumn("burst_year", F.year("burst_date"))
        .withColumn("burst_month", F.month("burst_date"))
        .withColumn("burst_day_of_year", F.dayofyear("burst_date"))
        .withColumn("burst_hour_utc", F.hour("burst_time"))
        # How long GCN took to publish after the burst -- the headline latency
        # metric for a follow-up observing programme.
        .withColumn(
            "notice_latency_seconds",
            F.unix_timestamp("notice_time") - F.unix_timestamp("burst_time"),
        )
        .dropDuplicates(["timing_key"])
    )


def build_fact(silver: DataFrame) -> DataFrame:
    latest = Window.partitionBy("trigger_num").orderBy(F.col("record_num").desc())
    return (
        silver.withColumn(
            "timing_key", _surrogate_key("burst_time", "notice_time")
        )
        .withColumn(
            "spatial_key",
            _surrogate_key(
                "ra_deg",
                "dec_deg",
                "error_radius_deg",
                "galactic_lon_deg",
                "galactic_lat_deg",
                "ecliptic_lon_deg",
                "ecliptic_lat_deg",
            ),
        )
        .withColumn("is_latest_revision", F.row_number().over(latest) == 1)
        .select(
            "notice_key",
            "trigger_num",
            "record_num",
            "timing_key",
            "spatial_key",
            "error_radius_deg",
            "phi_deg",
            "theta_deg",
            "e_range",
            "loc_algorithm",
            "lc_url",
            "loc_url",
            "comments",
            "is_latest_revision",
            "ingested_at",
        )
        # A replayed Kafka offset can deliver the same revision twice; the
        # business key is what makes the fact table idempotent.
        .dropDuplicates(["notice_key"])
    )


def _merge(spark: SparkSession, frame: DataFrame, target: str, key: str) -> None:
    """Upsert into the Gold table, creating it on first run."""
    if not spark.catalog.tableExists(target):
        frame.write.format("delta").saveAsTable(target)
        print(f"Created {target} ({frame.count()} rows)")
        return

    view = f"updates_{key}"
    frame.createOrReplaceTempView(view)
    columns = ", ".join(f"t.{c} = s.{c}" for c in frame.columns)
    spark.sql(
        f"""
        MERGE INTO {target} AS t
        USING {view} AS s ON t.{key} = s.{key}
        WHEN MATCHED THEN UPDATE SET {columns}
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    print(f"Merged into {target}")


def main() -> None:
    spark = configure_session(SparkSession.builder.getOrCreate())
    catalog = CatalogConfig()
    silver = spark.read.table(catalog.table("silver", SILVER_TABLE))

    for name, frame, key in (
        (DIM_SUN_MOON, build_sun_moon_dimension(silver), "sun_moon_key"),
        (DIM_SPATIAL, build_spatial_dimension(silver), "spatial_key"),
        (DIM_TIMING, build_timing_dimension(silver), "timing_key"),
        (FACT_TABLE, build_fact(silver), "notice_key"),
    ):
        _merge(spark, frame, catalog.table("gold", name), key)


if __name__ == "__main__":
    main()
