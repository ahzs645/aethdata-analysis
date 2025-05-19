"""
spartan_db_builder.py (lite)
===========================
Build an SQLite database from just the two combined spreadsheets:

* **SPARTAN_FTIR_&_HIPS_FTIR_Batches_3_4_5_SAMPLE.csv**
* **SPARTAN_FTIR_&_HIPS_FTIR_Batches_3_4_5_BLANK.csv**

The file names are hard‑coded below; adjust `FILE_PATHS` if you move them.

Usage
-----
    python spartan_db_builder.py                 # builds spartan_ftir_hips.db
    python spartan_db_builder.py --db my.db      # custom output name

Requires
--------
    Python ≥ 3.8, pandas
"""

from __future__ import annotations
import argparse
import sqlite3
from pathlib import Path
from typing import Dict, Optional, Sequence

import pandas as pd

###############################################################################
# CONFIG
###############################################################################

FILE_PATHS: Dict[str, str] = {
    "sample": "/Users/ahzs645/Library/CloudStorage/GoogleDrive-ahzs645@gmail.com/My Drive/University/Research/Grad/UC Davis Ann/NASA MAIA/Data/EC-HIPS-Aeth Comparison/Data/Original Data/New Combined Batch/SPARTAN_FTIR_&_HIPS_FTIR_Batches_3_4_5_SAMPLE.csv",
    "blank":  "/Users/ahzs645/Library/CloudStorage/GoogleDrive-ahzs645@gmail.com/My Drive/University/Research/Grad/UC Davis Ann/NASA MAIA/Data/EC-HIPS-Aeth Comparison/Data/Original Data/New Combined Batch/SPARTAN_FTIR_&_HIPS_FTIR_Batches_3_4_5_BLANK.csv",
}



NA_VALUES = {"NA", "NaN", "nan", "N/A", ""}

# This section is no longer necessary as we'll use actual column names from the file
# COL_RENAME = {
#     "FTIR_OC":          "OC_ftir",
#     "FTIR_EC":          "EC_ftir",
#     "OC_MDL":           "OC_ftir_MDL",
#     "EC_MDL":           "EC_ftir_MDL",
#     "Fabs_MDL":         "Fabs_MDL",
#     "Fabs_Uncertainty": "Fabs_Uncertainty",
#     "filter_id":        "FilterId",        # lower-case variant
# }


###############################################################################
# HELPER FUNCTIONS
###############################################################################

def normalize_header(name: str) -> str:
    """
    Strip leading/trailing whitespace, collapse runs of
    whitespace to a single underscore, and make case-exact
    so 'OC MDL', ' oc mdl ' → 'OC_MDL'.
    """
    import re, unicodedata
    # replace NO-BREAK SPACE with normal space
    name = name.replace('\u00A0', ' ')
    # strip & collapse whitespace → underscore
    name = re.sub(r'\s+', '_', name.strip())
    return name

def read_csv(path: Path) -> pd.DataFrame:
    """
    Read a CSV file and handle NA values and header normalization.
    """
    df = pd.read_csv(
        path,
        na_values=NA_VALUES,
        keep_default_na=True,
        dtype=str,
    )
    # Clean every raw header
    df.columns = [normalize_header(c) for c in df.columns]
    return df

def as_iso(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")

def to_float(value):
    return pd.to_numeric(value, errors="coerce")

###############################################################################
# DATABASE
###############################################################################

def create_schema(conn: sqlite3.Connection):
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS sites (
            site_code TEXT PRIMARY KEY,
            latitude  REAL,
            longitude REAL
        );

        CREATE TABLE IF NOT EXISTS filters (
            filter_id   TEXT PRIMARY KEY,
            site_code   TEXT REFERENCES sites(site_code),
            sample_date DATE,
            filter_type TEXT
        );

        CREATE TABLE IF NOT EXISTS ftir_sample_measurements (
            measurement_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filter_id      TEXT REFERENCES filters(filter_id),
            volume_m3      REAL,
            oc_ftir        REAL,
            oc_ftir_mdl    REAL,
            ec_ftir        REAL,
            ec_ftir_mdl    REAL,
            fabs           REAL,
            fabs_mdl       REAL,
            fabs_uncertainty REAL,
            ftir_batch_id  INTEGER
        );

        CREATE TABLE IF NOT EXISTS ftir_blank_measurements (
            filter_id    TEXT PRIMARY KEY REFERENCES filters(filter_id),
            oc_ftir      REAL,
            ec_ftir      REAL,
            tau          REAL,
            ftir_batch_id INTEGER
        );
        """
    )

###############################################################################
# IMPORT
###############################################################################

def import_data(conn: sqlite3.Connection, files: Dict[str, Path]):
    sample_df = read_csv(files["sample"])
    blank_df  = read_csv(files["blank"])

    # standardise date
    if "SampleDate" in sample_df:
        sample_df["SampleDate"] = as_iso(sample_df["SampleDate"])
    if "SampleDate" in blank_df:
        blank_df["SampleDate"] = as_iso(blank_df["SampleDate"])

    ###########################################################################
    # 1. sites  (sample + blank)
    ###########################################################################
    site_frames = [sample_df[["Site", "Latitude", "Longitude"]]]
    if "Site" in blank_df.columns:
        site_frames.append(blank_df[["Site"]].assign(Latitude=None, Longitude=None))

    sites = (
        pd.concat(site_frames, axis=0, ignore_index=True)
        .dropna(subset=["Site"])
        .drop_duplicates()
        .rename(columns={"Site": "site_code"})
    )

    with conn:
        conn.executemany(
            "INSERT OR IGNORE INTO sites(site_code, latitude, longitude) VALUES (?,?,?)",
            [tuple(r) for r in sites.itertuples(index=False, name=None)],
        )

    ###########################################################################
    # 2. filters  (sample + blank)
    ###########################################################################
    filters_cols = ["FilterId", "Site", "SampleDate", "FilterType"]

    sample_filters = sample_df[filters_cols]
    blank_filters  = blank_df[filters_cols] if set(filters_cols).issubset(blank_df.columns) else pd.DataFrame(columns=filters_cols)

    filters = (
        pd.concat([sample_filters, blank_filters], ignore_index=True)
        .dropna(subset=["FilterId"])
        .drop_duplicates()
        .rename(columns={
            "FilterId": "filter_id",
            "Site": "site_code",
            "SampleDate": "sample_date",
            "FilterType": "filter_type",
        })
    )

    with conn:
        conn.executemany(
            "INSERT OR IGNORE INTO filters(filter_id, site_code, sample_date, filter_type) VALUES (?,?,?,?)",
            [tuple(r) for r in filters.itertuples(index=False, name=None)],
        )

    ###########################################################################
    # 3. FTIR / HIPS sample measurements (from SAMPLE sheet only)
    ###########################################################################
    # THIS SECTION WAS REDUNDANT AND IS NOW REMOVED
    # filters_cols = ["FilterId", "Site", "SampleDate", "FilterType"]
    # filters = (
    #     sample_df[filters_cols]
    #     .dropna(subset=["FilterId"])
    #     .drop_duplicates()
    #     .rename(columns={
    #         "FilterId": "filter_id",
    #         "Site": "site_code",
    #         "SampleDate": "sample_date",
    #         "FilterType": "filter_type",
    #     })
    # )
    # with conn:
    #     conn.executemany(
    #         "INSERT OR IGNORE INTO filters(filter_id, site_code, sample_date, filter_type) VALUES (?,?,?,?)",
    #         [tuple(r) for r in filters.itertuples(index=False, name=None)],
    #     )

    ###########################################################################
    # 3. FTIR / HIPS sample measurements (from SAMPLE sheet only)
    ###########################################################################
    sample_rows = []
    for _, r in sample_df.iterrows():
        sample_rows.append(
            (
                r.get("FilterId"),
                to_float(r.get("Volume_m3")),
                to_float(r.get("OC_ftir")),         # Fixed: use actual column name
                to_float(r.get("OC_ftir_MDL")),     # Fixed: use actual column name
                to_float(r.get("EC_ftir")),         # Fixed: use actual column name
                to_float(r.get("EC_ftir_MDL")),     # Fixed: use actual column name
                to_float(r.get("Fabs")),
                to_float(r.get("Fabs_MDL")),
                to_float(r.get("Fabs_Uncertainty")),
                to_float(r.get("FTIRBatchId")),
            )
        )
    with conn:
        conn.executemany(
            """
            INSERT INTO ftir_sample_measurements
            (filter_id, volume_m3, oc_ftir, oc_ftir_mdl, ec_ftir, ec_ftir_mdl,
             fabs, fabs_mdl, fabs_uncertainty, ftir_batch_id)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            sample_rows,
        )

    ###########################################################################
    # 4. FTIR blanks (from BLANK sheet only)
    ###########################################################################
    blank_rows = []
    for _, r in blank_df.iterrows():
        blank_rows.append(
            (
                r.get("FilterId"),
                to_float(r.get("OC_ftir")),
                to_float(r.get("EC_ftir")),
                to_float(r.get("tau")),
                to_float(r.get("FTIRBatchId")),
            )
        )
    with conn:
        conn.executemany(
            """
            INSERT INTO ftir_blank_measurements
            (filter_id, oc_ftir, ec_ftir, tau, ftir_batch_id)
            VALUES (?,?,?,?,?)
            """,
            blank_rows,
        )

###############################################################################
# DRIVER
###############################################################################

def build_database(db_path: Path, data_dir: Path):
    # resolve file paths
    files: Dict[str, Path] = {}
    for key, fname in FILE_PATHS.items():
        path = (data_dir / fname) if not Path(fname).is_absolute() else Path(fname)
        if not path.exists():
            raise FileNotFoundError(f"Missing required file: {path}")
        files[key] = path

    with sqlite3.connect(db_path) as conn:
        create_schema(conn)
        import_data(conn, files)
        print("✅ built", db_path)

###############################################################################
# CLI
###############################################################################

def parse_args(argv: Sequence[str] | None = None):
    p = argparse.ArgumentParser(description="Build FTIR/HIPS DB (batches 3‑5 only)")
    p.add_argument("--db", default="spartan_ftir_hips.db", help="output SQLite file")
    p.add_argument("--data-dir", default=".", help="directory of the two CSV files")
    return p.parse_args(argv)

if __name__ == "__main__":
    args = parse_args()
    build_database(Path(args.db), Path(args.data_dir))