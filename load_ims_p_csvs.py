"""
load_ims_p_csvs.py
──────────────────────────────────────────────────────────────────────────────
Loads all CSV files exported from ims_be_p.accdb into PostgreSQL.
Tables are prefixed with 'p_' to identify their source.

Usage:
    python load_ims_p_csvs.py

Configuration: edit DB_* and CSV_DIR below if needed.
──────────────────────────────────────────────────────────────────────────────
"""

import os
import sys
import re
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
DB_HOST  = "localhost"
DB_PORT  = 5433
DB_NAME  = "sales_db"
DB_USER  = os.popen("whoami").read().strip()
DB_PASS  = ""
CSV_DIR  = Path.home() / "Desktop" / "ims_p_csvs"

# ── HELPERS ───────────────────────────────────────────────────────────────────

def clean_table_name(filename: str) -> str:
    """Convert CSV filename to a clean PostgreSQL table name with p_ prefix."""
    name = Path(filename).stem
    name = re.sub(r"[^a-zA-Z0-9]", "_", name).lower()
    name = re.sub(r"_+", "_", name).strip("_")
    return f"p_{name}"


def infer_dtype(series: pd.Series) -> str:
    """Infer a basic PostgreSQL type from a pandas Series."""
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(series):
        return "DOUBLE PRECISION"
    else:
        return "TEXT"


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "═" * 60)
    print("  IMS-P CSV Loader → PostgreSQL")
    print("═" * 60)

    # Connect
    print(f"\n[1/3] Connecting to {DB_NAME} on port {DB_PORT}...")
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("      ✓ Connected")
    except Exception as e:
        print(f"      ✗ Connection failed: {e}")
        sys.exit(1)

    # Find CSV files
    print(f"\n[2/3] Scanning {CSV_DIR}...")
    csv_files = sorted(CSV_DIR.glob("*.csv"))
    if not csv_files:
        print(f"      ✗ No CSV files found in {CSV_DIR}")
        sys.exit(1)
    print(f"      Found {len(csv_files)} CSV files")

    # Load each CSV
    print(f"\n[3/3] Loading tables...")
    success = 0
    failed  = 0
    total_rows = 0

    for csv_path in csv_files:
        table_name = clean_table_name(csv_path.name)
        try:
            df = pd.read_csv(
                csv_path,
                encoding        = "latin-1",
                low_memory      = False,
                on_bad_lines    = "skip"
            )

            if df.empty:
                print(f"    → {table_name:<45} 0 rows (empty, skipped)")
                continue

            # Clean column names
            df.columns = [
                re.sub(r"[^a-zA-Z0-9]", "_", c).lower().strip("_")
                for c in df.columns
            ]
            df.columns = [re.sub(r"_+", "_", c) for c in df.columns]

            # Write to PostgreSQL
            df.to_sql(
                table_name,
                engine,
                if_exists = "replace",
                index     = False,
                method    = "multi",
                chunksize = 500
            )

            total_rows += len(df)
            success    += 1
            print(f"    → {table_name:<45} {len(df):>6,} rows  ✓")

        except Exception as e:
            failed += 1
            print(f"    → {table_name:<45} FAILED: {e}")

    # Summary
    print(f"\n{'═' * 60}")
    print(f"  LOAD COMPLETE")
    print(f"{'═' * 60}")
    print(f"  Tables loaded:  {success}")
    print(f"  Tables failed:  {failed}")
    print(f"  Total rows:     {total_rows:,}")
    print(f"\n  All p_* tables are now in sales_db.")
    print(f"  Run the Phase 2 query in pgAdmin next.\n")


if __name__ == "__main__":
    main()
