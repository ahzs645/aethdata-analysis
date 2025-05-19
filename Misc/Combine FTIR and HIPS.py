"""
spartan_db_builder.py
=====================
Builds an SQLite database that consolidates SPARTAN FTIR & HIPS data.

Usage
-----
    python spartan_db_builder.py                       # creates spartan_ftir_hips.db
    python spartan_db_builder.py --db path/to/db.sqlite

Requires
--------
    pandas, sqlite3 (std‑lib), python ≥ 3.8
"""

import argparse
import re
import sqlite3
from pathlib import Path
from typing import Dict, Optional, Sequence

import pandas as pd

###############################################################################
# CONFIG
###############################################################################

# Absolute paths to the five CSVs on your machine ------------------------------------------------
FILE_PATTERNS: Dict[str, str] = {
    "sample": "/Users/ahzs645/Library/CloudStorage/GoogleDrive-ahzs645@gmail.com/My Drive/University/Research/Grad/UC Davis Ann/NASA MAIA/Data/EC-HIPS-Aeth Comparison/Data/Original Data/New Combined Batch/SPARTAN_FTIR_&_HIPS_FTIR_Batches_3_4_5_SAMPLE.csv",
    "blank": "/Users/ahzs645/Library/CloudStorage/GoogleDrive-ahzs645@gmail.com/My Drive/University/Research/Grad/UC Davis Ann/NASA MAIA/Data/EC-HIPS-Aeth Comparison/Data/Original Data/New Combined Batch/SPARTAN_FTIR_&_HIPS_FTIR_Batches_3_4_5_BLANK.csv",
    "etad": "/Users/ahzs645/Library/CloudStorage/GoogleDrive-ahzs645@gmail.com/My Drive/University/Research/Grad/UC Davis Ann/NASA MAIA/Data/EC-HIPS-Aeth Comparison/Data/Original Data/HIPS BC/ETAD_data_2023.csv",
    "batch23": "/Users/ahzs645/Library/CloudStorage/GoogleDrive-ahzs645@gmail.com/My Drive/University/Research/Grad/UC Davis Ann/NASA MAIA/Data/EC-HIPS-Aeth Comparison/Data/Original Data/FTIR EC/SPARTAN_FTIR data batch 2 and 3 resubmitted with MDLs.csv",
    "batch4": "/Users/ahzs645/Library/CloudStorage/GoogleDrive-ahzs645@gmail.com/My Drive/University/Research/Grad/UC Davis Ann/NASA MAIA/Data/EC-HIPS-Aeth Comparison/Data/Original Data/FTIR EC/SPARTAN_FTIR data_batch 4_ Nov2022 to March2024.csv",
}

# Values that should be treated as NULL
NA_VALUES = {"NA", "NaN", "nan", "N/A", ""}

# Regex to turn the long sample string into a canonical filter code (SITE-NNNN-P)
FILTER_RE = re.compile(r"_(?P<site>[A-Z]{4})_(?P<num>\d{4})_(?P<port>\d)_")

###############################################################################
# HELPER FUNCTIONS
###############################################################################

def locate_files(directory: Path | str) -> Dict[str, Optional[Path]]:
    """Return mapping of logical keys → Path (or None if missing)."""
    result: Dict[str, Optional[Path]] = {}
    for key, pattern in FILE_PATTERNS.items():
        path = Path(pattern)
        result[key] = path if path.exists() else None
    return result


def read_csv(path: Path, **kwargs) -> pd.DataFrame:
    """Read a CSV with NA handling."""
    return pd.read_csv(path, na_values=NA_VALUES, keep_default_na=True, dtype=str, **kwargs)


def as_iso(col: pd.Series) -> pd.Series:
    """Convert column of date‑like strings → YYYY‑MM‑DD."""
    dt = pd.to_datetime(col, errors="coerce")
    return dt.dt.strftime("%Y-%m-%d")


def to_float(series: pd.Series | str | None):
    return pd.to_numeric(series, errors="coerce") if isinstance(series, pd.Series) else pd.to_numeric(series, errors="coerce")


def sample_to_filter(sample: str) -> Optional[str]:
    """Tensor II_SN151_SM_ETAD_0042_2_PM2_5_02_13_2023_0_csv → ETAD-0042-2"""
    if not isinstance(sample, str):
        return None
    m = FILTER_RE.search(sample)
    if not m:
        return None
    return f"{m.group('site')}-{m.group('num')}-{m.group('port')}"



###############################################################################
# DATABASE BUILD
###############################################################################


def create_tables(cursor: sqlite3.Cursor) -> None:
    """
    Create the normalized schema.
    """
    cursor.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS sites (
            site_code   TEXT PRIMARY KEY,
            latitude    REAL,
            longitude   REAL,
            site_name   TEXT
        );

        CREATE TABLE IF NOT EXISTS filters (
            filter_id            TEXT PRIMARY KEY,
            barcode              TEXT,
            site_code            TEXT REFERENCES sites(site_code),
            sample_date          DATE,
            filter_type          TEXT,
            lot_id               INTEGER,
            project_id           TEXT,
            external_shipment_id TEXT,
            filter_comments      TEXT
        );

        CREATE TABLE IF NOT EXISTS ftir_sample_measurements (
            measurement_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filter_id      TEXT REFERENCES filters(filter_id),
            ftir_batch_id  INTEGER,
            sample_id      TEXT,
            volume_m3      REAL,
            oc_ftir        REAL,
            oc_ftir_mdl    REAL,
            ec_ftir        REAL,
            ec_ftir_mdl    REAL,
            comments       TEXT
        );

        CREATE TABLE IF NOT EXISTS ftir_blank_measurements (
            filter_id     TEXT PRIMARY KEY REFERENCES filters(filter_id),
            ftir_batch_id INTEGER,
            oc_ftir       REAL,
            ec_ftir       REAL,
            tau           REAL,
            comments      TEXT
        );

        CREATE TABLE IF NOT EXISTS hips_measurements (
            measurement_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            filter_id        TEXT REFERENCES filters(filter_id),
            analysis_date    DATE,
            analysis_time    TEXT,
            t1               REAL,
            r1               REAL,
            intercept        REAL,
            slope            REAL,
            t                REAL,
            r                REAL,
            tau              REAL,
            deposit_area     REAL,
            volume           REAL,
            fabs             REAL,
            fabs_mdl         REAL,
            fabs_uncertainty REAL,
            analysis_comments TEXT,
            ftir_batch_id    INTEGER
        );

        CREATE TABLE IF NOT EXISTS functional_groups (
            measurement_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filter_id      TEXT REFERENCES filters(filter_id),
            sample_id      TEXT,
            acoh           REAL,
            ach            REAL,
            naco           REAL,
            cooh           REAL,
            om             REAL
        );

        CREATE INDEX IF NOT EXISTS idx_filters_site        ON filters(site_code);
        CREATE INDEX IF NOT EXISTS idx_ftir_sample_filter  ON ftir_sample_measurements(filter_id);
        CREATE INDEX IF NOT EXISTS idx_hips_filter         ON hips_measurements(filter_id);
        CREATE INDEX IF NOT EXISTS idx_sample_id           ON ftir_sample_measurements(sample_id);
        """
    )


def insert_sites(df_list: Sequence[pd.DataFrame], conn: sqlite3.Connection) -> None:
    """
    Extract unique site rows from provided DataFrames and insert.
    """
    sites_cols = ["Site", "Latitude", "Longitude"]
    rows = []
    for df in df_list:
        if df is None or not set(sites_cols).issubset(df.columns):
            continue
        tmp = (
            df[sites_cols]
            .dropna(subset=["Site"])
            .drop_duplicates()
            .rename(columns={"Site": "site_code", "Latitude": "lat", "Longitude": "lon"})
        )
        rows.extend(
            tmp.apply(
                lambda r: (r["site_code"], to_float(r["lat"]), to_float(r["lon"]), None),
                axis=1,
            ).tolist()
        )

    with conn:
        conn.executemany(
            "INSERT OR IGNORE INTO sites(site_code, latitude, longitude, site_name) VALUES (?, ?, ?, ?)",
            rows,
        )


def import_data(files: Dict[str, Optional[Path]], conn: sqlite3.Connection) -> None:
    """
    Load CSVs → DataFrames → insert into DB.
    """
    # Read all existing CSVs ---------------------------------------------------
    dfs = {k: (read_csv(p) if p else None) for k, p in files.items()}

    # Standardise key date columns upfront
    if dfs["sample"] is not None and "SampleDate" in dfs["sample"]:
        dfs["sample"]["SampleDate"] = as_iso(dfs["sample"]["SampleDate"])
    if dfs["blank"] is not None and "SampleDate" in dfs["blank"]:
        dfs["blank"]["SampleDate"] = as_iso(dfs["blank"]["SampleDate"])
    if dfs["etad"] is not None and "AnalysisDate" in dfs["etad"]:
        dfs["etad"]["AnalysisDate"] = as_iso(dfs["etad"]["AnalysisDate"])

    # ---------- 1. sites ------------------------------------------------------
    insert_sites(
        [dfs["sample"], dfs["etad"], dfs["batch23"], dfs["batch4"]],
        conn,
    )

    # ---------- 2. filters ----------------------------------------------------
    filter_rows = []
    for key in ("sample", "blank", "etad"):
        df = dfs[key]
        if df is None:
            continue
        for _, r in df.iterrows():
            fid = r.get("FilterId")
            if pd.isna(fid):
                continue
            filter_rows.append(
                (
                    fid,
                    str(r.get("Barcode")) if pd.notna(r.get("Barcode")) else None,
                    r.get("Site") if pd.notna(r.get("Site")) else None,
                    r.get("SampleDate") if pd.notna(r.get("SampleDate")) else None,
                    r.get("FilterType") if pd.notna(r.get("FilterType")) else None,
                    r.get("LotId") if pd.notna(r.get("LotId")) else None,
                    r.get("ProjectId") if pd.notna(r.get("ProjectId")) else None,
                    r.get("ExternalShipmentId") if pd.notna(r.get("ExternalShipmentId")) else None,
                    r.get("FilterComments") if pd.notna(r.get("FilterComments")) else None,
                )
            )

    with conn:
        conn.executemany(
            """
            INSERT OR IGNORE INTO filters
            (filter_id, barcode, site_code, sample_date, filter_type, lot_id,
             project_id, external_shipment_id, filter_comments)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            filter_rows,
        )

    # ---------- 3. FTIR sample measurements ----------------------------------
    sample_rows = []
    df = dfs["sample"]
    if df is not None:
        for _, r in df.iterrows():
            if pd.isna(r.get("FilterId")) and pd.isna(r.get("OC_ftir")) and pd.isna(
                r.get("EC_ftir")
            ):
                continue
            sample_rows.append(
                (
                    r.get("FilterId"),
                    r.get("FTIRBatchId"),
                    None,
                    to_float(r.get("Volume_m3")),
                    to_float(r.get("OC_ftir")),
                    to_float(r.get("OC_ftir_MDL")),
                    to_float(r.get("EC_ftir")),
                    to_float(r.get("EC_ftir_MDL")),
                    r.get("HIPSComments"),
                )
            )

    # From batch files (no filter_id) -----------------------------------------
    for key, batch_id in (("batch4", 4), ("batch23", None)):
        dfb = dfs[key]
        if dfb is None:
            continue
        for _, r in dfb.iterrows():
            if pd.isna(r.get("sample")):
                continue
            oc = r.get("FTIR_OC")
            ec = r.get("FTIR_EC")
            if oc in NA_VALUES and ec in NA_VALUES:
                continue
            sample_rows.append(
                (
                    None,  # filter_id
                    batch_id,
                    r.get("sample"),
                    to_float(r.get("volume")),
                    to_float(oc),
                    to_float(r.get("OC MDL")),
                    to_float(ec),
                    to_float(r.get("EC MDL")),
                    None,
                )
            )

    with conn:
        conn.executemany(
            """
            INSERT INTO ftir_sample_measurements
            (filter_id, ftir_batch_id, sample_id, volume_m3, oc_ftir, oc_ftir_mdl,
             ec_ftir, ec_ftir_mdl, comments)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            sample_rows,
        )

    # ---------- 4. FTIR blanks -----------------------------------------------
    blank_rows = []
    df = dfs["blank"]
    if df is not None:
        for _, r in df.iterrows():
            blank_rows.append(
                (
                    r.get("FilterId"),
                    r.get("FTIRBatchId"),
                    to_float(r.get("OC_ftir")),
                    to_float(r.get("EC_ftir")),
                    to_float(r.get("tau")),
                    r.get("HIPSComments"),
                )
            )

    with conn:
        conn.executemany(
            """
            INSERT INTO ftir_blank_measurements
            (filter_id, ftir_batch_id, oc_ftir, ec_ftir, tau, comments)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            blank_rows,
        )

    # ---------- 5. functional groups -----------------------------------------
    fg_rows = []
    for key in ("batch4", "batch23"):
        dfb = dfs[key]
        if dfb is None:
            continue
        for _, r in dfb.iterrows():
            if pd.isna(r.get("sample")):
                continue
            values = [r.get(col) for col in ("aCOH", "aCH", "naCO", "COOH", "OM")]
            if all(v in NA_VALUES or pd.isna(v) for v in values):
                continue
            fg_rows.append(
                (
                    None,  # filter_id
                    r.get("sample"),
                    *map(to_float, values),
                )
            )

    with conn:
        conn.executemany(
            """
            INSERT INTO functional_groups
            (filter_id, sample_id, acoh, ach, naco, cooh, om)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            fg_rows,
        )

    # ---------- 6. HIPS measurements -----------------------------------------
    hips_rows = []
    df = dfs["sample"]
    if df is not None:
        for _, r in df.iterrows():
            if pd.isna(r.get("FilterId")) or pd.isna(r.get("Fabs")):
                continue
            hips_rows.append(
                (
                    r.get("FilterId"),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    to_float(r.get("Volume_m3")),
                    to_float(r.get("Fabs")),
                    to_float(r.get("Fabs_MDL")),
                    to_float(r.get("Fabs_Uncertainty")),
                    r.get("HIPSComments"),
                    r.get("FTIRBatchId"),
                )
            )

    df = dfs["etad"]
    if df is not None:
        for _, r in df.iterrows():
            if pd.isna(r.get("FilterId")):
                continue
            hips_rows.append(
                (
                    r.get("FilterId"),
                    r.get("AnalysisDate"),
                    r.get("AnalysisTime"),
                    to_float(r.get("T1")),
                    to_float(r.get("R1")),
                    to_float(r.get("Intercept")),
                    to_float(r.get("Slope")),
                    to_float(r.get("t")),
                    to_float(r.get("r")),
                    to_float(r.get("tau")),
                    to_float(r.get("DepositArea")),
                    to_float(r.get("Volume")),
                    to_float(r.get("Fabs")),
                    to_float(r.get("MDL")),
                    to_float(r.get("Uncertainty")),
                    r.get("AnalysisComments"),
                    None,
                )
            )

    with conn:
        conn.executemany(
            """
            INSERT INTO hips_measurements
            (filter_id, analysis_date, analysis_time, t1, r1, intercept, slope,
             t, r, tau, deposit_area, volume, fabs, fabs_mdl, fabs_uncertainty,
             analysis_comments, ftir_batch_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            hips_rows,
        )

###############################################################################
# IMPORT & LINKING
###############################################################################

# insert_sites, import_data functions are as before UNTIL *after* we bulk‑insert
# ftir_sample_measurements. Then we call link_sample_rows(conn) to patch NULL
# filter_ids and (optionally) create placeholder filters.


def link_sample_rows(conn: sqlite3.Connection, create_missing_filters: bool = True) -> None:
    """Back-fill filter_id from sample_id and create placeholder filters + sites."""
    cur = conn.cursor()

    # ------------------------------------------------------------------ helpers
    def ensure_sites_exist(codes: set[str]):
        """Insert stub rows into `sites` for any missing site codes."""
        if not codes:
            return
        cur.execute(
            f"SELECT site_code FROM sites WHERE site_code IN ({','.join('?'*len(codes))})",
            tuple(codes),
        )
        existing = {row[0] for row in cur.fetchall()}
        missing = codes - existing
        if missing:
            cur.executemany("INSERT INTO sites(site_code) VALUES (?)",
                            [(sc,) for sc in missing])

    def ensure_filters_exist(filters: list[tuple]):
        """Insert placeholder filters and any missing parent sites."""
        if not filters:
            return
        site_codes = {tpl[2] for tpl in filters}           # site_code is 3rd element
        ensure_sites_exist(site_codes)
        cur.executemany(
            """INSERT OR IGNORE INTO filters
               (filter_id, barcode, site_code, sample_date, filter_type,
                lot_id, project_id, external_shipment_id, filter_comments)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            filters,
        )

    # ------------------------------------------------------------------ FTIR samples
    cur.execute("""
        SELECT measurement_id, sample_id
        FROM ftir_sample_measurements
        WHERE filter_id IS NULL AND sample_id IS NOT NULL
    """)
    updates, new_filters = [], []
    for mid, sample in cur.fetchall():
        fid = sample_to_filter(sample)
        if not fid:
            continue
        cur.execute("SELECT 1 FROM filters WHERE filter_id = ? LIMIT 1", (fid,))
        if not cur.fetchone() and create_missing_filters:
            site_code = fid.split("-")[0]
            new_filters.append((fid, None, site_code, None, None, None, None, None, None))
        updates.append((fid, mid))

    ensure_filters_exist(new_filters)

    if updates:
        cur.executemany(
            "UPDATE ftir_sample_measurements SET filter_id = ? WHERE measurement_id = ?",
            updates,
        )

    # ------------------------------------------------------------------ functional groups
    cur.execute("""
        SELECT measurement_id, sample_id
        FROM functional_groups
        WHERE filter_id IS NULL AND sample_id IS NOT NULL
    """)
    fg_updates, fg_new_filters = [], []
    for mid, sample in cur.fetchall():
        fid = sample_to_filter(sample)
        if not fid:
            continue
        cur.execute("SELECT 1 FROM filters WHERE filter_id = ? LIMIT 1", (fid,))
        if not cur.fetchone() and create_missing_filters:
            site_code = fid.split("-")[0]
            fg_new_filters.append((fid, None, site_code, None, None, None, None, None, None))
        fg_updates.append((fid, mid))

    ensure_filters_exist(fg_new_filters)

    if fg_updates:
        cur.executemany(
            "UPDATE functional_groups SET filter_id = ? WHERE measurement_id = ?",
            fg_updates,
        )

    conn.commit()




###############################################################################
# MAIN
###############################################################################


def build_database(db_path: Path, data_dir: Path):
    files = locate_files(data_dir)
    missing = [k for k, v in files.items() if v is None]
    if missing:
        print("⚠️  Missing input files:", ", ".join(missing))

    with sqlite3.connect(db_path) as conn:
        create_tables(conn)
        import_data(files, conn)
        link_sample_rows(conn)  # <—— NEW STEP
        print(f"✅ Database created at {db_path.resolve()}")


###############################################################################
# CLI
###############################################################################

def parse_args(argv: Sequence[str] | None = None):
    p = argparse.ArgumentParser(
        description="Build SPARTAN FTIR/HIPS SQLite database with auto‑linking"
    )
    p.add_argument("--db", default="spartan_ftir_hips.db", help="Output DB path")
    p.add_argument("--data-dir", default=".", help="Directory containing CSVs")
    return p.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    build_database(Path(args.db), Path(args.data_dir))
