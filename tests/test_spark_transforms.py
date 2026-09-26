"""Spark-side tests for the Silver and Gold transforms.

These run on a local Spark session against the same fixtures as the parser
tests, so the UDF binding, the declared schema and the dimensional logic are
exercised without a cluster, a Kafka broker or credentials. Skipped when pyspark
is not installed.
"""

from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "pipelines"))

from pyspark.sql import SparkSession  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"

# Spark's Python workers are separate processes and do not inherit the driver's
# sys.path, so the UDF's module has to reach them through the environment. On a
# cluster this is handled by installing the package or calling
# gcn_lakehouse.deploy.ship_to_executors.
os.environ["PYTHONPATH"] = os.pathsep.join(
    [str(ROOT / "src"), str(ROOT / "pipelines"), os.environ.get("PYTHONPATH", "")]
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("gcn-lakehouse-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def bronze(spark):
    """A Bronze-shaped DataFrame built from the fixture notices."""
    from pyspark.sql.functions import current_timestamp

    rows = []
    for offset, name in enumerate(
        ["fermi_gbm_fin_pos.txt", "wrapped_and_comments.txt", "malformed.txt"]
    ):
        rows.append(
            (
                (FIXTURES / name).read_text(encoding="utf-8"),
                None,
                "gcn.classic.text.FERMI_GBM_FIN_POS",
                0,
                offset,
                dt.datetime(2025, 8, 14, 12, 0, 0),
            )
        )
    return spark.createDataFrame(
        rows,
        "payload string, message_key string, topic string, "
        "partition int, offset long, kafka_timestamp timestamp",
    ).withColumn("ingested_at", current_timestamp())


@pytest.fixture(scope="module")
def silver(bronze):
    from silver_parse import parse_bronze

    return parse_bronze(bronze).cache()


class TestSilverTransform:
    def test_udf_produces_declared_schema(self, silver):
        from gcn_lakehouse.schemas import SILVER_FIELD_NAMES

        assert set(SILVER_FIELD_NAMES).issubset(set(silver.columns))

    def test_parsed_and_malformed_notices_are_separated(self, silver):
        assert silver.where("is_parsed").count() == 2
        assert silver.where("not is_parsed").count() == 1

    def test_values_survive_the_arrow_round_trip(self, silver):
        row = silver.where("trigger_num = 776751152").collect()[0]
        assert row.ra_deg == pytest.approx(214.517)
        assert row.dec_deg == pytest.approx(-11.300)
        assert row.comments == "Fermi-GBM Final Position. This is a GRB."

    def test_burst_instant_is_stored_as_utc(self, silver):
        # Asserted through Spark rather than collect(): collect() renders a
        # timestamp as a naive datetime in the *driver's* local zone, which
        # would make this assertion pass or fail based on where it is run.
        from pyspark.sql.functions import date_format

        rendered = (
            silver.where("trigger_num = 776751152")
            .select(date_format("burst_time", "yyyy-MM-dd HH:mm:ss").alias("utc"))
            .collect()[0]
            .utc
        )
        assert rendered == "2025-08-13 04:05:12"

    def test_kafka_metadata_is_retained(self, silver):
        row = silver.where("trigger_num = 776751152").collect()[0]
        assert row.topic == "gcn.classic.text.FERMI_GBM_FIN_POS"
        assert row.offset == 0

    def test_rescued_fields_reach_spark_as_a_map(self, silver):
        row = silver.where("trigger_num = 776840000").collect()[0]
        assert row.rescued_fields == {"UNKNOWN_FUTURE": "some new field value"}


class TestGoldModel:
    @pytest.fixture(scope="class")
    def clean(self, silver):
        return silver.where("is_parsed")

    def test_fact_grain_is_one_row_per_notice_revision(self, clean):
        from gold_dimensional import build_fact

        fact = build_fact(clean)
        assert fact.count() == 2
        assert fact.select("notice_key").distinct().count() == 2

    def test_surrogate_keys_are_deterministic_across_runs(self, clean):
        from gold_dimensional import build_fact

        first = {r.notice_key: r.spatial_key for r in build_fact(clean).collect()}
        second = {r.notice_key: r.spatial_key for r in build_fact(clean).collect()}
        assert first == second

    def test_fact_keys_join_to_every_dimension(self, clean):
        from gold_dimensional import (
            build_fact,
            build_spatial_dimension,
            build_sun_moon_dimension,
            build_timing_dimension,
        )

        fact = build_fact(clean)
        spatial = build_spatial_dimension(clean)
        timing = build_timing_dimension(clean)
        sun_moon = build_sun_moon_dimension(clean)

        # No orphan foreign keys in either direction of the snowflake.
        assert fact.join(spatial, "spatial_key").count() == fact.count()
        assert fact.join(timing, "timing_key").count() == fact.count()
        assert spatial.join(sun_moon, "sun_moon_key").count() == spatial.count()

    def test_latest_revision_flag_marks_one_row_per_trigger(self, clean):
        from gold_dimensional import build_fact

        latest = build_fact(clean).where("is_latest_revision")
        assert latest.count() == latest.select("trigger_num").distinct().count()

    def test_burst_hour_is_utc_not_cluster_local(self, clean):
        # hour() reads spark.sql.session.timeZone, so an unpinned cluster in any
        # other zone yields a different burst_hour_utc for the same instant.
        from gold_dimensional import build_timing_dimension

        row = build_timing_dimension(clean).where("burst_day_of_year = 225").collect()[0]
        assert row.burst_hour_utc == 4

    def test_timing_dimension_computes_publication_latency(self, clean):
        from gold_dimensional import build_timing_dimension

        row = (
            build_timing_dimension(clean)
            .where("burst_year = 2025 and burst_day_of_year = 225")
            .collect()[0]
        )
        # Burst at 04:05:12, notice at 04:12:33 -> 441 seconds.
        assert row.notice_latency_seconds == 441
