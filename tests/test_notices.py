"""Tests for classic-text parsing.

The cases here are the ones a colon-splitting or ``str_to_map`` parser gets
wrong: colons inside values, coordinates wrapped over three lines, repeated
``COMMENTS`` keys, and non-breaking spaces.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from gcn_lakehouse import build_record, parse_notice

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def notice() -> str:
    return load("fermi_gbm_fin_pos.txt")


@pytest.fixture
def awkward() -> str:
    return load("wrapped_and_comments.txt")


class TestParseNotice:
    def test_splits_on_first_colon_only(self, notice):
        # GRB_TIME embeds {04:05:12.00}; splitting on every colon loses it.
        assert parse_notice(notice)["GRB_TIME"] == "14712.00 SOD {04:05:12.00} UT"

    def test_url_value_survives_its_scheme_colon(self, notice):
        loc = parse_notice(notice)["LOC_URL"]
        assert loc.startswith("http://heasarc.gsfc.nasa.gov/")
        assert loc.endswith("glg_locplot_all_bn250813170.png")

    def test_wrapped_coordinate_lines_fold_into_one_value(self, notice):
        ra = parse_notice(notice)["GRB_RA"]
        assert "\n" not in ra
        # All three epochs are retained on the folded line.
        assert "(J2000)" in ra and "(current)" in ra and "(1950)" in ra

    def test_repeated_comments_are_joined_in_order(self, notice):
        assert parse_notice(notice)["COMMENTS"] == (
            "Fermi-GBM Final Position. This is a GRB."
        )

    def test_many_comment_lines_all_survive(self, awkward):
        comments = parse_notice(awkward)["COMMENTS"]
        assert "SAA passage" in comments
        assert "Human-in-the-loop review pending." in comments

    def test_non_breaking_spaces_are_normalised(self, awkward):
        assert parse_notice(awkward)["TITLE"] == "GCN/FERMI NOTICE"

    def test_unknown_field_is_kept(self, awkward):
        assert parse_notice(awkward)["UNKNOWN_FUTURE"] == "some new field value"

    @pytest.mark.parametrize("text", ["", "   ", load("malformed.txt")])
    def test_unparseable_input_returns_empty_without_raising(self, text):
        assert parse_notice(text) == {}


class TestBuildRecord:
    def test_extracts_j2000_position(self, notice):
        record = build_record(notice)
        assert record["ra_deg"] == pytest.approx(214.517)
        assert record["dec_deg"] == pytest.approx(-11.300)

    def test_error_radius_drops_its_units(self, notice):
        assert build_record(notice)["error_radius_deg"] == pytest.approx(1.75)

    def test_burst_timestamp_from_tjd_and_seconds_of_day(self, notice):
        # TJD 20900 is 2025-08-13; 14712 SOD is 04:05:12 UTC.
        assert build_record(notice)["burst_time"] == dt.datetime(
            2025, 8, 13, 4, 5, 12, tzinfo=dt.timezone.utc
        )

    def test_notice_timestamp_parsed_as_utc(self, notice):
        assert build_record(notice)["notice_time"] == dt.datetime(
            2025, 8, 13, 4, 12, 33, tzinfo=dt.timezone.utc
        )

    def test_galactic_and_ecliptic_pairs(self, notice):
        record = build_record(notice)
        assert record["galactic_lon_deg"] == pytest.approx(337.06)
        assert record["galactic_lat_deg"] == pytest.approx(47.88)
        assert record["ecliptic_lat_deg"] == pytest.approx(3.28)

    def test_sun_and_moon_positions_read_both_degree_values(self, notice):
        record = build_record(notice)
        assert record["sun_ra_deg"] == pytest.approx(143.21)
        assert record["sun_dec_deg"] == pytest.approx(14.32)
        assert record["moon_dec_deg"] == pytest.approx(-12.04)

    def test_sun_dist_ignores_trailing_sun_angle(self, notice):
        # The value trails with "Sun_angle= -4.7 [hr]"; only the distance counts.
        assert build_record(notice)["sun_dist_deg"] == pytest.approx(73.44)

    def test_notice_key_pairs_trigger_and_record(self, notice):
        assert build_record(notice)["notice_key"] == "776751152-52"

    def test_unknown_fields_are_rescued_not_dropped(self, awkward):
        assert build_record(awkward)["rescued_fields"] == {
            "UNKNOWN_FUTURE": "some new field value"
        }

    def test_malformed_notice_is_flagged_rather_than_raising(self):
        record = build_record(load("malformed.txt"))
        assert record["is_parsed"] is False
        assert record["notice_key"] is None

    def test_record_shape_is_stable_for_empty_input(self):
        from gcn_lakehouse import record_columns

        assert set(build_record("")) == set(record_columns())
