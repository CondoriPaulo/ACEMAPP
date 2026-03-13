import io
import logging
import os
from typing import Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Unit_Mapping — loaded once per worker lifetime (module-level cache)
# ---------------------------------------------------------------------------

_UNIT_MAPPING: Optional[dict] = None

_MAPPING_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "data",
    "unit_mapping.xlsx",
)


def _load_unit_mapping() -> dict:
    global _UNIT_MAPPING
    if _UNIT_MAPPING is not None:
        return _UNIT_MAPPING

    path = os.environ.get("UNIT_MAPPING_PATH") or _MAPPING_FILE
    if not os.path.exists(path):
        logging.warning(
            "unit_mapping.xlsx not found at '%s'. Unit names will not be normalized.", path
        )
        _UNIT_MAPPING = {}
        return _UNIT_MAPPING

    try:
        df = pd.read_excel(
            path,
            sheet_name="Unit_Mapping",
            usecols=[0, 1],
            header=0,
            dtype=str,
            engine="openpyxl",
        )
        df.columns = ["raw", "clean"]
        df = df.dropna(subset=["raw"])
        df["raw"] = df["raw"].str.strip()
        df["clean"] = df["clean"].str.strip()
        _UNIT_MAPPING = dict(zip(df["raw"], df["clean"]))
        logging.info("Unit_Mapping loaded: %d entries", len(_UNIT_MAPPING))
    except Exception as exc:
        logging.error("Failed to load unit_mapping.xlsx: %s", exc)
        _UNIT_MAPPING = {}

    return _UNIT_MAPPING


# ---------------------------------------------------------------------------
# Status normalization
# ---------------------------------------------------------------------------

_STATUS_MAP = {
    "archived (completed)": "Completed",
    "archived (approved)":  "Completed",
    "approved":             "Completed",
    "completed":            "Completed",
    "archived (denied)":    "Denied",
    "denied":               "Denied",
    "archived (withdrawn)": "Withdrawn",
    "withdrawn":            "Withdrawn",
    "pending":              "Pending",
}


def _normalize_status(raw) -> tuple:
    """Return (status_raw, status_clean). status_clean is None if unrecognized."""
    if pd.isna(raw):
        return (None, None)
    raw_str = str(raw).strip()
    clean = _STATUS_MAP.get(raw_str.lower())
    if clean is None:
        logging.warning("ACEMAPP: unrecognized status value: '%s'", raw_str)
    return (raw_str, clean)


# ---------------------------------------------------------------------------
# Cohort derivation
# ---------------------------------------------------------------------------

def _derive_cohort(ts) -> str:
    """Map a start_date timestamp to an academic cohort label."""
    if pd.isna(ts):
        return ""
    m = ts.month
    y = ts.year
    if m <= 5:
        return f"Spring {y}"
    if m <= 7:
        return f"Summer {y}"
    return f"Fall {y}"


# ---------------------------------------------------------------------------
# Main cleaner
# ---------------------------------------------------------------------------

_COLUMN_RENAME = {
    "Rotation ID":    "rotation_id",
    "Schools":        "school_name",
    "Sites":          "site_name",
    "Unit":           "unit_raw",
    "Program":        "program",
    "Start Date":     "start_date",
    "End Date":       "end_date",
    "Status":         "status_raw",
    "Student Count":  "student_count",
    "Student Slots":  "student_slots",
}

_OUTPUT_COLS = [
    "rotation_id",
    "school_name",
    "site_name",
    "unit_clean",
    "program",
    "start_date",
    "end_date",
    "status_raw",
    "status_clean",
    "student_count",
    "student_slots",
    "cohort",
    "year",
]


def clean_acemapp(file_bytes: bytes) -> pd.DataFrame:
    """
    Accept raw Excel bytes. Return a clean DataFrame whose columns
    match fact_rotation exactly.

    Logic App POSTs the file as application/octet-stream; this function
    receives those raw bytes and returns the cleaned data. It never
    touches Azure SQL — that is the Logic App's job.
    """
    # Step 1: Read Excel — dtype=str so we control all type coercion
    df = pd.read_excel(
        io.BytesIO(file_bytes),
        sheet_name="rotation_schedule_deid_23-25v2",
        dtype=str,
        engine="openpyxl",
    )

    # Step 2: Drop blank rows (3,769 rows in the source file have no Rotation ID)
    before = len(df)
    df = df.dropna(subset=["Rotation ID"])
    after = len(df)
    logging.info("ACEMAPP: dropped %d blank rows, kept %d", before - after, after)

    # Step 3: Rename columns to snake_case
    df = df.rename(columns=_COLUMN_RENAME)

    # Step 4: Apply Unit_Mapping; fall back to raw name if no mapping found
    mapping = _load_unit_mapping()
    df["unit_clean"] = df["unit_raw"].str.strip().map(mapping)
    unmapped = df[df["unit_clean"].isna()]["unit_raw"].dropna().unique()
    for u in unmapped:
        logging.warning("ACEMAPP: unmapped unit name: '%s'", u)
    df["unit_clean"] = df["unit_clean"].fillna(df["unit_raw"].str.strip())

    # Step 5: Normalize status → (status_raw, status_clean)
    status_pairs = df["status_raw"].apply(
        lambda v: pd.Series(_normalize_status(v), index=["status_raw", "status_clean"])
    )
    df[["status_raw", "status_clean"]] = status_pairs

    # Step 6: Parse dates
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.date
    df["end_date"]   = pd.to_datetime(df["end_date"],   errors="coerce").dt.date

    # Step 7: Parse student counts to nullable integer
    df["student_count"]  = pd.to_numeric(df["student_count"].str.strip(),  errors="coerce").astype("Int64")
    df["student_slots"]  = pd.to_numeric(df["student_slots"].str.strip(),  errors="coerce").astype("Int64")

    # Step 8: Title-case site names
    df["site_name"] = df["site_name"].str.strip().str.title()

    # Step 9: Derive cohort label and calendar year from start_date
    start_ts = pd.to_datetime(df["start_date"], errors="coerce")
    df["cohort"] = start_ts.apply(_derive_cohort)
    df["year"]   = start_ts.dt.year.astype("Int64")

    # Step 10: Select and order output columns
    return df[_OUTPUT_COLS].copy()
