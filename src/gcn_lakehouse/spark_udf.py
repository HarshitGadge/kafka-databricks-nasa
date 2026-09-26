"""Spark binding for the pure-Python notice parser.

The parser is exposed as an Arrow-vectorised pandas UDF rather than a row-at-a-
time Python UDF: the JVM hands over a whole column per call, so per-row
serialisation cost is paid once per batch instead of once per notice.
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import Column
from pyspark.sql.functions import pandas_udf

from .records import build_record
from .schemas import SILVER_FIELD_NAMES, SILVER_NOTICE


@pandas_udf(SILVER_NOTICE)
def parse_notice_udf(payloads: pd.Series) -> pd.DataFrame:
    """Parse a column of raw notice text into a struct column."""
    parsed = [build_record(text or "") for text in payloads]
    frame = pd.DataFrame(parsed, columns=list(SILVER_FIELD_NAMES))
    # Spark matches struct fields by position for a pandas UDF returning a
    # DataFrame, so the column order must equal the declared schema order.
    return frame[list(SILVER_FIELD_NAMES)]


def parsed_notice(payload: Column) -> Column:
    """Convenience wrapper: ``df.withColumn("notice", parsed_notice(col))``."""
    return parse_notice_udf(payload)
