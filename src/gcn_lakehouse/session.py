"""Spark session settings this pipeline depends on for correctness."""

from __future__ import annotations

#: Every timestamp GCN publishes is UTC, and the parser emits tz-aware UTC
#: datetimes. Spark's TimestampType stores an instant but renders and derives
#: calendar parts in ``spark.sql.session.timeZone``, which defaults to the JVM's
#: local zone. Left unset on a cluster in any other zone, ``burst_hour_utc`` and
#: ``burst_day_of_year`` in Gold come out shifted -- silently, since the values
#: stay internally consistent.
SESSION_TIMEZONE = "UTC"


def configure_session(spark):
    """Apply the settings the medallion tables assume. Returns the session."""
    spark.conf.set("spark.sql.session.timeZone", SESSION_TIMEZONE)
    # Arrow is what makes the vectorised parser worth using; without it the
    # pandas UDF silently falls back to row-at-a-time serialisation.
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    return spark
