#!/usr/bin/env python3
"""Run the pipeline's transforms against sample notices, with no credentials.

    python demo.py

Parses the bundled fixture notices and prints what each layer produces. The
Silver section needs nothing but Python; the Gold section runs on a local Spark
session and is skipped if pyspark is not installed.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "pipelines"))

from gcn_lakehouse import build_record, parse_notice  # noqa: E402

FIXTURES = ROOT / "tests" / "fixtures"
RULE = "=" * 72


def header(text: str) -> None:
    print(f"\n{RULE}\n  {text}\n{RULE}")


def show_raw(notice: str) -> None:
    header("BRONZE  raw classic-text notice, as GCN delivered it")
    for line in notice.splitlines()[:14]:
        print(f"  {line}")
    print("  ...")


def show_hazards(notice: str) -> None:
    header("PARSING  the cases that defeat a naive colon split")
    fields = parse_notice(notice)
    checks = [
        ("colon inside a value", "GRB_TIME", fields["GRB_TIME"]),
        ("colon in a URL", "LOC_URL", fields["LOC_URL"][:46] + "..."),
        ("wrapped over 3 lines", "GRB_RA", fields["GRB_RA"][:46] + "..."),
        ("repeated key, joined", "COMMENTS", fields["COMMENTS"]),
    ]
    for label, key, value in checks:
        print(f"  [ok] {label:22s}  {key:9s} -> {value}")


def show_silver(notice: str) -> None:
    header("SILVER  typed columns extracted from the notice")
    record = build_record(notice)
    groups = {
        "identity": ("notice_key", "trigger_num", "record_num"),
        "timing": ("burst_time", "notice_time"),
        "position (J2000)": ("ra_deg", "dec_deg", "error_radius_deg"),
        "galactic": ("galactic_lon_deg", "galactic_lat_deg"),
        "sun / moon": ("sun_dist_deg", "moon_dist_deg", "moon_illum_pct"),
    }
    for group, names in groups.items():
        print(f"  {group}")
        for name in names:
            print(f"    {name:20s} {record[name]}")


def show_quarantine() -> None:
    header("SILVER  malformed input is flagged, not dropped")
    bad = build_record((FIXTURES / "malformed.txt").read_text(encoding="utf-8"))
    print(f"    is_parsed            {bad['is_parsed']}")
    print(f"    notice_key           {bad['notice_key']}")
    print("    -> routed to fermi_gbm_fin_pos_quarantine with its payload")

    odd = build_record((FIXTURES / "wrapped_and_comments.txt").read_text(encoding="utf-8"))
    print(f"\n    unmodelled GCN field rescued, not lost:")
    print(f"    rescued_fields       {odd['rescued_fields']}")


def show_gold() -> None:
    header("GOLD  dimensional model (local Spark)")
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        print("  pyspark not installed - skipping.")
        print("  pip install -r requirements-dev.txt to run this section.")
        return

    import os

    os.environ.setdefault(
        "PYTHONPATH", os.pathsep.join([str(ROOT / "src"), str(ROOT / "pipelines")])
    )
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("gcn-demo")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    from pyspark.sql.functions import current_timestamp

    from gold_dimensional import build_fact, build_spatial_dimension, build_timing_dimension
    from silver_parse import parse_bronze

    rows = [
        ((FIXTURES / name).read_text(encoding="utf-8"), None,
         "gcn.classic.text.FERMI_GBM_FIN_POS", 0, i, None)
        for i, name in enumerate(
            ["fermi_gbm_fin_pos.txt", "wrapped_and_comments.txt", "malformed.txt"]
        )
    ]
    bronze = spark.createDataFrame(
        rows,
        "payload string, message_key string, topic string, "
        "partition int, offset long, kafka_timestamp timestamp",
    ).withColumn("ingested_at", current_timestamp())

    silver = parse_bronze(bronze).where("is_parsed").cache()
    print(f"\n  fact_gcn_notices  (grain: one notice revision)")
    build_fact(silver).select(
        "notice_key", "trigger_num", "record_num", "error_radius_deg", "is_latest_revision"
    ).show(truncate=False)

    print("  dim_event_timing  (notice_latency_seconds = GCN publish delay)")
    build_timing_dimension(silver).select(
        "burst_time", "burst_hour_utc", "burst_day_of_year", "notice_latency_seconds"
    ).show(truncate=False)

    print("  dim_spatial_coords  (deterministic hash keys, abbreviated here)")
    from pyspark.sql.functions import substring

    build_spatial_dimension(silver).select(
        substring("spatial_key", 1, 12).alias("spatial_key"),
        "ra_deg", "dec_deg", "galactic_lon_deg", "error_radius_deg",
    ).show(truncate=False)

    spark.stop()


def main() -> None:
    notice = (FIXTURES / "fermi_gbm_fin_pos.txt").read_text(encoding="utf-8")
    print("\n  NASA GCN Fermi Streaming Lakehouse - offline demo")
    print("  No credentials or cluster required.")
    show_raw(notice)
    show_hazards(notice)
    show_silver(notice)
    show_quarantine()
    show_gold()
    print(f"\n{RULE}\n  Done. See docs/DATA_MODEL.md for the full Gold schema.\n{RULE}\n")


if __name__ == "__main__":
    main()
