"""
Tests for cleaners/acemapp_cleaner.py

Fixtures are synthetic — no real patient or student data.
Run with: pytest tests/ -v --tb=short
"""
import io
import os
from unittest.mock import patch

import pandas as pd
import pytest

# Allow imports from project root when running pytest from ACEMAPP/
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cleaners.acemapp_cleaner import clean_acemapp, _derive_cohort, _normalize_status

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "acemapp_sample.xlsx")

_MOCK_MAPPING = {
    "MICU":                "Medical ICU",
    "21: ICU":             "21: Peachtree ICU",
    "21: General Surgery": "21: General Surgery",
}

OUTPUT_COLS = {
    "rotation_id", "school_name", "site_name", "unit_clean",
    "program", "start_date", "end_date", "status_raw", "status_clean",
    "student_count", "student_slots", "cohort", "year",
}


def _fixture_bytes() -> bytes:
    with open(FIXTURE, "rb") as f:
        return f.read()


def _run(mock_mapping=None) -> pd.DataFrame:
    mapping = mock_mapping if mock_mapping is not None else _MOCK_MAPPING
    with patch("cleaners.acemapp_cleaner._UNIT_MAPPING", mapping):
        return clean_acemapp(_fixture_bytes())


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_blank_row_removal():
    """Source fixture has 5 rows with Rotation IDs + 3 blank rows → output has 5 rows."""
    df = _run()
    assert len(df) == 5


def test_status_approved_to_completed():
    """'Approved' and 'Archived (Approved)' both normalize to 'Completed'."""
    df = _run()
    approved_rows = df[df["status_raw"].isin(["Approved", "Archived (Approved)"])]
    assert (approved_rows["status_clean"] == "Completed").all()


def test_status_archived_completed_to_completed():
    """'Archived (Completed)' normalizes to 'Completed'."""
    df = _run()
    row = df[df["rotation_id"] == "R001"]
    assert row.iloc[0]["status_clean"] == "Completed"


def test_status_denied():
    """'Archived (Denied)' normalizes to 'Denied'."""
    df = _run()
    row = df[df["rotation_id"] == "R003"]
    assert row.iloc[0]["status_clean"] == "Denied"


def test_status_withdrawn():
    """'Archived (Withdrawn)' normalizes to 'Withdrawn'."""
    df = _run()
    row = df[df["rotation_id"] == "R004"]
    assert row.iloc[0]["status_clean"] == "Withdrawn"


def test_status_unknown_produces_none(caplog):
    """Unknown status → status_clean is None; WARNING is logged."""
    import logging
    with caplog.at_level(logging.WARNING, logger="root"):
        df = _run()
    row = df[df["rotation_id"] == "R005"]
    assert row.iloc[0]["status_clean"] is None
    assert "UNKNOWN_STATUS" in caplog.text


def test_unit_mapping_applied():
    """Known unit names are replaced with clean names from the mapping."""
    df = _run()
    # R001 and R005 both have unit 'MICU' → should map to 'Medical ICU'
    micu_rows = df[df["rotation_id"].isin(["R001", "R005"])]
    assert (micu_rows["unit_clean"] == "Medical ICU").all()


def test_unit_mapping_fallback(caplog):
    """Unmapped unit names fall back to the raw name; WARNING is logged."""
    import logging
    with caplog.at_level(logging.WARNING, logger="root"):
        df = _run(mock_mapping={})
    row = df[df["rotation_id"] == "R001"]
    assert row.iloc[0]["unit_clean"] == "MICU"
    assert "MICU" in caplog.text


def test_unmapped_unit_logged(caplog):
    """Unit not in mapping table logs a WARNING."""
    import logging
    with caplog.at_level(logging.WARNING, logger="root"):
        df = _run()
    row = df[df["rotation_id"] == "R003"]
    assert row.iloc[0]["unit_clean"] == "unmapped_unit_xyz"
    assert "unmapped_unit_xyz" in caplog.text


def test_site_name_title_case():
    """Site names are normalized to title case."""
    df = _run()
    # R005 has site 'emory university hospital' (lowercase)
    row = df[df["rotation_id"] == "R005"]
    assert row.iloc[0]["site_name"] == "Emory University Hospital"


def test_student_count_parsed_to_int():
    """Member Count (stored as text) is parsed to a nullable integer."""
    df = _run()
    row = df[df["rotation_id"] == "R001"]
    assert row.iloc[0]["student_count"] == 8


def test_cohort_spring():
    """March start_date → 'Spring YYYY' cohort."""
    assert _derive_cohort(pd.Timestamp("2024-03-15")) == "Spring 2024"


def test_cohort_summer():
    """June start_date → 'Summer YYYY' cohort."""
    assert _derive_cohort(pd.Timestamp("2024-06-01")) == "Summer 2024"


def test_cohort_fall():
    """September start_date → 'Fall YYYY' cohort."""
    assert _derive_cohort(pd.Timestamp("2023-09-01")) == "Fall 2023"


def test_cohort_derived_in_dataframe():
    """Cohort and year columns are populated from start_date."""
    df = _run()
    row = df[df["rotation_id"] == "R001"]
    assert row.iloc[0]["cohort"] == "Spring 2024"
    assert row.iloc[0]["year"] == 2024


def test_output_columns_match_schema():
    """Output DataFrame has exactly the columns that match fact_rotation."""
    df = _run()
    assert set(df.columns) == OUTPUT_COLS


def test_no_nan_strings_in_output():
    """NaN values should be proper nulls, not the string 'nan'."""
    df = _run()
    rows = df.where(pd.notnull(df), None).to_dict(orient="records")
    for row in rows:
        for val in row.values():
            assert val != "nan", f"Found string 'nan' in output row: {row}"
