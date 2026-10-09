"""Source preparation: load the provided Grammy CSV into the operational source DB.

This is NOT the ETL Load stage (that is load_dw into music_dw). It only creates the
relational source the pipeline later extracts from (extract_grammys).

Safe to rerun: DDL is IF NOT EXISTS and the table is truncated and reloaded in a
single transaction, so a failed run leaves the previous content untouched.

Usage (from the project root, with the stack up):
    python src/load_grammy_source.py
"""
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))   # project root: works as script or module
from src import config

TABLE = "public.grammy_awards"
COLUMNS = ["year", "title", "published_at", "updated_at", "category",
           "nominee", "artist", "workers", "img", "winner"]


def sha256_of(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def csv_row_count(path) -> int:
    with open(path, newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f)) - 1  # minus header


def main() -> int:
    csv_path = config.GRAMMY_CSV
    if not csv_path.exists():
        print(f"ERROR: CSV not found: {csv_path}")
        return 1

    # Original file is only read, never modified. Empty field == NULL, same as COPY below.
    df = pd.read_csv(csv_path, keep_default_na=False, na_values=[""])
    if list(df.columns) != COLUMNS:
        print(f"ERROR: unexpected columns {list(df.columns)}")
        return 1
    csv_rows = csv_row_count(csv_path)
    csv_nulls = {c: int(df[c].isna().sum()) for c in COLUMNS}
    csv_winner_true = int((df["winner"].astype(str).str.lower() == "true").sum())

    conn = psycopg2.connect(**config.pg_params(config.SOURCE_DB))
    try:
        with conn, conn.cursor() as cur:   # one transaction: commit or rollback as a unit
            cur.execute((config.SQL_DIR / "source_setup.sql").read_text(encoding="utf-8"))
            cur.execute(f"TRUNCATE {TABLE} RESTART IDENTITY")
            with open(csv_path, encoding="utf-8") as f:
                cur.copy_expert(
                    f"COPY {TABLE} ({', '.join(COLUMNS)}) FROM STDIN "
                    "WITH (FORMAT csv, HEADER true, NULL '')", f)

            cur.execute(f"SELECT count(*) FROM {TABLE}")
            db_rows = cur.fetchone()[0]
            db_nulls = {}
            for c in COLUMNS:
                cur.execute(f"SELECT count(*) FROM {TABLE} WHERE {c} IS NULL")
                db_nulls[c] = cur.fetchone()[0]
            cur.execute(f"SELECT count(*) FROM {TABLE} WHERE winner IS TRUE")
            db_winner_true = cur.fetchone()[0]

            checks = {
                "row_count": {"csv": csv_rows, "db": db_rows, "ok": csv_rows == db_rows},
                "null_counts_per_column": {
                    c: {"csv": csv_nulls[c], "db": db_nulls[c], "ok": csv_nulls[c] == db_nulls[c]}
                    for c in COLUMNS},
                "winner_true_count": {"csv": csv_winner_true, "db": db_winner_true,
                                      "ok": csv_winner_true == db_winner_true},
            }
            reconciled = (checks["row_count"]["ok"] and checks["winner_true_count"]["ok"]
                          and all(v["ok"] for v in checks["null_counts_per_column"].values()))
            digest = sha256_of(csv_path)
            cur.execute(
                "INSERT INTO public.source_load_audit "
                "(csv_file, csv_sha256, csv_rows, db_rows, reconciled) VALUES (%s,%s,%s,%s,%s)",
                (csv_path.name, digest, csv_rows, db_rows, reconciled))
            if not reconciled:
                raise RuntimeError("Reconciliation failed; transaction rolled back")
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1
    finally:
        conn.close()

    report = {
        "run_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "csv_file": csv_path.name, "csv_sha256": digest,
        "target": f"{config.SOURCE_DB}.{TABLE}", "reconciled": reconciled, "checks": checks,
    }
    config.EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    out = config.EVIDENCE_DIR / "source_reconciliation.json"
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Loaded {db_rows} rows into {config.SOURCE_DB}.{TABLE} (CSV rows: {csv_rows}).")
    print(f"Reconciled: {reconciled}. Evidence: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())