"""Typed extraction of individual GCN notice values.

:mod:`gcn_lakehouse.notices` splits a notice into ``KEY -> str``. The values are
still prose: ``GRB_ERROR`` reads ``1.75 [deg radius, statistical only]`` and
``GRB_RA`` carries three epochs on one folded line. The helpers here turn those
into numbers and timestamps, and return ``None`` rather than raising when a
field is absent or malformed, so one unparseable value nulls one column instead
of failing a micro-batch.
"""

from __future__ import annotations

import datetime as _dt
import re
from typing import Dict, Optional, Tuple

# TJD (Truncated Julian Date) 0 is 1968-05-24. GCN reports it alongside a
# two-digit calendar date; TJD is preferred here because it carries no century
# ambiguity.
_TJD_EPOCH = _dt.date(1968, 5, 24)

_FLOAT = r"[-+]?\d+(?:\.\d+)?"
_DEGREES = re.compile(rf"({_FLOAT})\s*d\b")
_LEADING_FLOAT = re.compile(rf"^\s*({_FLOAT})")
_TJD = re.compile(rf"({_FLOAT})\s*TJD")
_SOD = re.compile(rf"({_FLOAT})\s*SOD")
_PAIR = re.compile(rf"({_FLOAT})\s*,\s*({_FLOAT})")


def _to_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def first_degrees(value: Optional[str]) -> Optional[float]:
    """Decimal degrees from the first epoch in a coordinate field.

    ``GRB_RA`` lists J2000, current and 1950 positions in that order; the J2000
    value is the one the Gold model stores, so the first match is the right one.
    """
    if not value:
        return None
    match = _DEGREES.search(value)
    return _to_float(match.group(1)) if match else None


def leading_float(value: Optional[str]) -> Optional[float]:
    """Numeric prefix of a field whose remainder is a unit or comment.

    Covers ``GRB_ERROR``, ``SUN_DIST``, ``MOON_ILLUM``, ``GRB_PHI`` and friends,
    all of which lead with the number and trail with bracketed prose.
    """
    if not value:
        return None
    match = _LEADING_FLOAT.match(value)
    return _to_float(match.group(1)) if match else None


def coordinate_pair(value: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    """Longitude/latitude pair from ``GAL_COORDS`` or ``ECL_COORDS``."""
    if not value:
        return (None, None)
    match = _PAIR.search(value)
    if not match:
        return (None, None)
    return (_to_float(match.group(1)), _to_float(match.group(2)))


def position_pair(value: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    """RA/Dec pair from ``SUN_POSTN`` or ``MOON_POSTN``.

    Both degree values are suffixed with ``d``, so unlike ``GAL_COORDS`` the
    pair is read from the degree markers rather than a comma.
    """
    if not value:
        return (None, None)
    matches = _DEGREES.findall(value)
    if len(matches) < 2:
        return (None, None)
    return (_to_float(matches[0]), _to_float(matches[1]))


def integer(value: Optional[str]) -> Optional[int]:
    """Leading integer of a field such as ``TRIGGER_NUM`` or ``RECORD_NUM``."""
    number = leading_float(value)
    return int(number) if number is not None else None


def burst_date(value: Optional[str]) -> Optional[_dt.date]:
    """Calendar date from the TJD component of ``GRB_DATE``."""
    if not value:
        return None
    match = _TJD.search(value)
    if not match:
        return None
    tjd = _to_float(match.group(1))
    if tjd is None:
        return None
    return _TJD_EPOCH + _dt.timedelta(days=int(tjd))


def burst_timestamp(date_value: Optional[str], time_value: Optional[str]) -> Optional[_dt.datetime]:
    """UTC instant of the burst, from ``GRB_DATE`` plus ``GRB_TIME``.

    ``GRB_TIME`` gives seconds-of-day, which is used in preference to the
    ``{hh:mm:ss.ss}`` gloss beside it because it keeps sub-second precision.
    """
    day = burst_date(date_value)
    if day is None or not time_value:
        return None
    match = _SOD.search(time_value)
    if not match:
        return None
    seconds = _to_float(match.group(1))
    if seconds is None:
        return None
    return _dt.datetime.combine(day, _dt.time(), _dt.timezone.utc) + _dt.timedelta(seconds=seconds)


def notice_timestamp(value: Optional[str]) -> Optional[_dt.datetime]:
    """Parse ``NOTICE_DATE``, e.g. ``Wed 13 Aug 25 04:12:33 UT``."""
    if not value:
        return None
    cleaned = value.replace("UT", "").strip()
    for fmt in ("%a %d %b %y %H:%M:%S", "%a %d %b %Y %H:%M:%S"):
        try:
            parsed = _dt.datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
        return parsed.replace(tzinfo=_dt.timezone.utc)
    return None


def trigger_key(fields: Dict[str, str]) -> Optional[str]:
    """Business key for a notice: trigger number plus record number.

    GCN re-issues a notice for the same trigger as the localisation is refined,
    incrementing ``RECORD_NUM``. The pair identifies one revision uniquely, and
    is what the Gold fact table deduplicates on.
    """
    trigger = integer(fields.get("TRIGGER_NUM"))
    record = integer(fields.get("RECORD_NUM"))
    if trigger is None:
        return None
    return f"{trigger}-{record}" if record is not None else str(trigger)
