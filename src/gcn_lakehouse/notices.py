"""Parsing for NASA GCN classic-text notices.

GCN classic notices are newline-delimited ``KEY: value`` records written for
human readers, not machines. Three properties of the format defeat a naive
``split(':')`` or a Spark ``str_to_map`` call:

* Values contain colons. ``GRB_TIME`` embeds ``{04:05:12.00}`` and ``LOC_URL``
  is an ``http://`` address, so only the *first* colon delimits key from value.
* Values wrap across lines. ``GRB_RA`` and ``GRB_DEC`` carry the J2000, current
  and 1950 epochs on three physical lines; the continuations are indented and
  carry no key of their own.
* Keys repeat. A notice may hold any number of ``COMMENTS:`` lines, which a map
  keyed by field name cannot represent without discarding all but one.

Everything here is pure Python over ``str`` so it can be unit-tested without a
Spark session. :mod:`gcn_lakehouse.spark_udf` wraps it for use on a cluster.
"""

from __future__ import annotations

import re
from typing import Dict, List

# A key line starts at column zero with an uppercase label and a colon. The
# anchor matters: continuation lines are indented, which is exactly what tells
# them apart from a new field.
_KEY_LINE = re.compile(r"^([A-Z][A-Z0-9_]*):(.*)$")

# Fields whose repeated occurrences are joined rather than overwritten.
_REPEATABLE = frozenset({"COMMENTS"})

# Non-breaking spaces appear in some notices and survive ordinary strip() calls,
# leaving values that compare unequal to their visible text.
_NBSP = " "


def parse_notice(text: str) -> Dict[str, str]:
    """Parse one classic-text notice into a field mapping.

    Unknown fields are preserved: the caller decides which to project into a
    table, so a new GCN field shows up in the rescued column rather than being
    silently dropped at parse time.

    Repeated ``COMMENTS`` lines are joined with a single space, in order.
    Malformed input yields an empty mapping rather than raising -- a streaming
    job should quarantine a bad record, not fail the batch.
    """
    if not text:
        return {}

    fields: Dict[str, List[str]] = {}
    current: str | None = None

    for raw_line in text.replace(_NBSP, " ").splitlines():
        line = raw_line.rstrip()
        if not line.strip():
            continue

        match = _KEY_LINE.match(line)
        if match:
            key, value = match.group(1), match.group(2).strip()
            fields.setdefault(key, []).append(value)
            current = key
        elif current is not None:
            # Continuation of the previous field: fold it onto one line.
            fields[current][-1] = f"{fields[current][-1]} {line.strip()}".strip()
        # A continuation with no preceding key is leading junk; skip it.

    out: Dict[str, str] = {}
    for key, values in fields.items():
        if key in _REPEATABLE:
            out[key] = " ".join(v for v in values if v)
        else:
            # Last write wins for an unexpected duplicate, matching the way a
            # reader scanning top-to-bottom would read the notice.
            out[key] = values[-1]
    return out
