"""Assembly of a parsed notice into the flat Silver record."""

from __future__ import annotations

from typing import Any, Dict

from . import fields as f
from .notices import parse_notice

# Fields projected verbatim as text. Anything outside this set and the derived
# columns below lands in ``rescued_fields`` instead of being dropped.
_TEXT_FIELDS = (
    "TITLE",
    "NOTICE_TYPE",
    "E_RANGE",
    "LOC_ALGORITHM",
    "LC_URL",
    "LOC_URL",
    "COMMENTS",
)

_DERIVED_FROM = frozenset(
    {
        "NOTICE_DATE",
        "RECORD_NUM",
        "TRIGGER_NUM",
        "GRB_RA",
        "GRB_DEC",
        "GRB_ERROR",
        "GRB_DATE",
        "GRB_TIME",
        "GRB_PHI",
        "GRB_THETA",
        "SUN_POSTN",
        "SUN_DIST",
        "MOON_POSTN",
        "MOON_DIST",
        "MOON_ILLUM",
        "GAL_COORDS",
        "ECL_COORDS",
    }
)


def build_record(text: str) -> Dict[str, Any]:
    """Turn one raw classic-text notice into the Silver row.

    The returned mapping always carries every key, with ``None`` for fields the
    notice omitted, so the shape is stable enough to hand straight to Spark.
    """
    raw = parse_notice(text)

    gal_lon, gal_lat = f.coordinate_pair(raw.get("GAL_COORDS"))
    ecl_lon, ecl_lat = f.coordinate_pair(raw.get("ECL_COORDS"))
    sun_ra, sun_dec = f.position_pair(raw.get("SUN_POSTN"))
    moon_ra, moon_dec = f.position_pair(raw.get("MOON_POSTN"))

    record: Dict[str, Any] = {
        "notice_key": f.trigger_key(raw),
        "trigger_num": f.integer(raw.get("TRIGGER_NUM")),
        "record_num": f.integer(raw.get("RECORD_NUM")),
        "notice_time": f.notice_timestamp(raw.get("NOTICE_DATE")),
        "burst_time": f.burst_timestamp(raw.get("GRB_DATE"), raw.get("GRB_TIME")),
        "burst_date": f.burst_date(raw.get("GRB_DATE")),
        "ra_deg": f.first_degrees(raw.get("GRB_RA")),
        "dec_deg": f.first_degrees(raw.get("GRB_DEC")),
        "error_radius_deg": f.leading_float(raw.get("GRB_ERROR")),
        "phi_deg": f.leading_float(raw.get("GRB_PHI")),
        "theta_deg": f.leading_float(raw.get("GRB_THETA")),
        "galactic_lon_deg": gal_lon,
        "galactic_lat_deg": gal_lat,
        "ecliptic_lon_deg": ecl_lon,
        "ecliptic_lat_deg": ecl_lat,
        "sun_ra_deg": sun_ra,
        "sun_dec_deg": sun_dec,
        "sun_dist_deg": f.leading_float(raw.get("SUN_DIST")),
        "moon_ra_deg": moon_ra,
        "moon_dec_deg": moon_dec,
        "moon_dist_deg": f.leading_float(raw.get("MOON_DIST")),
        "moon_illum_pct": f.leading_float(raw.get("MOON_ILLUM")),
    }
    for name in _TEXT_FIELDS:
        record[name.lower()] = raw.get(name)

    known = _DERIVED_FROM.union(_TEXT_FIELDS)
    record["rescued_fields"] = {k: v for k, v in raw.items() if k not in known} or None
    record["is_parsed"] = record["notice_key"] is not None
    return record


def record_columns() -> tuple[str, ...]:
    """Column order of a Silver record, for schema assertions and tests."""
    return tuple(build_record("").keys())
