"""Spark schemas for the Silver and Gold layers.

Declared explicitly rather than inferred: a streaming job that infers its schema
will change table shape the first time GCN emits a notice with a new field, and
Delta will reject the write mid-stream.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

#: Kafka metadata carried through Bronze so a record can always be traced back
#: to its offset on the broker.
KAFKA_METADATA = StructType(
    [
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False),
        StructField("kafka_timestamp", TimestampType(), True),
    ]
)

#: Output of :func:`gcn_lakehouse.records.build_record`.
SILVER_NOTICE = StructType(
    [
        StructField("notice_key", StringType(), True),
        StructField("trigger_num", LongType(), True),
        StructField("record_num", IntegerType(), True),
        StructField("notice_time", TimestampType(), True),
        StructField("burst_time", TimestampType(), True),
        StructField("burst_date", DateType(), True),
        StructField("ra_deg", DoubleType(), True),
        StructField("dec_deg", DoubleType(), True),
        StructField("error_radius_deg", DoubleType(), True),
        StructField("phi_deg", DoubleType(), True),
        StructField("theta_deg", DoubleType(), True),
        StructField("galactic_lon_deg", DoubleType(), True),
        StructField("galactic_lat_deg", DoubleType(), True),
        StructField("ecliptic_lon_deg", DoubleType(), True),
        StructField("ecliptic_lat_deg", DoubleType(), True),
        StructField("sun_ra_deg", DoubleType(), True),
        StructField("sun_dec_deg", DoubleType(), True),
        StructField("sun_dist_deg", DoubleType(), True),
        StructField("moon_ra_deg", DoubleType(), True),
        StructField("moon_dec_deg", DoubleType(), True),
        StructField("moon_dist_deg", DoubleType(), True),
        StructField("moon_illum_pct", DoubleType(), True),
        StructField("title", StringType(), True),
        StructField("notice_type", StringType(), True),
        StructField("e_range", StringType(), True),
        StructField("loc_algorithm", StringType(), True),
        StructField("lc_url", StringType(), True),
        StructField("loc_url", StringType(), True),
        StructField("comments", StringType(), True),
        StructField("rescued_fields", MapType(StringType(), StringType()), True),
        StructField("is_parsed", BooleanType(), True),
    ]
)

SILVER_FIELD_NAMES = tuple(field.name for field in SILVER_NOTICE.fields)

__all__ = [
    "KAFKA_METADATA",
    "SILVER_NOTICE",
    "SILVER_FIELD_NAMES",
    "ArrayType",
]
