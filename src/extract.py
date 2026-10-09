"""Extraction (6.7). Spotify comes from the CSV file; Grammy comes from the relational source database
(grammy_source.public.grammy_awards), never from the Grammy CSV.

Task interface: each extract function stages the raw data as Parquet and returns a *compact metadata dict*
(batch id, path, row count, source). Bulk data never travels through Airflow's metadata store (XCom).
No cleaning happens here: source-quality problems stay visible for the raw validation gate.
"""
import hashlib
import json
import logging
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from src import config

log = logging.getLogger(__name__)

GRAMMY_QUERY = "SELECT * FROM public.grammy_awards ORDER BY source_row_id"


def new_batch_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def staging_dir(batch_id: str):
    d = config.STAGING_DIR / batch_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _sha256(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_evidence(batch_id: str, name: str, payload: dict) -> None:
    d = config.EVIDENCE_DIR / "extract" / batch_id
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{name}.json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def read_spotify_csv(csv_path=None) -> pd.DataFrame:
    path = csv_path or config.SPOTIFY_CSV
    return pd.read_csv(path)


def read_grammy_table() -> pd.DataFrame:
    p = config.pg_params(config.SOURCE_DB)
    engine = create_engine(URL.create("postgresql+psycopg2", username=p["user"], password=p["password"],
                                      host=p["host"], port=p["port"], database=p["dbname"]))
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(GRAMMY_QUERY), conn)
    finally:
        engine.dispose()


def extract_spotify(batch_id: str, csv_path=None) -> dict:
    source_file = csv_path or config.SPOTIFY_CSV
    df = read_spotify_csv(source_file)
    path = staging_dir(batch_id) / "spotify_raw.parquet"
    df.to_parquet(path, index=False)
    meta = {"batch_id": batch_id, "dataset": "spotify_raw", "path": str(path), "rows": int(len(df)),
            "source": f"CSV file {source_file.name if hasattr(source_file, 'name') else str(source_file)}",
            "source_sha256": _sha256(source_file)}
    _write_evidence(batch_id, "spotify_extract", meta)
    log.info("[%s] extract_spotify: %s rows from %s -> %s", batch_id, meta["rows"], meta["source"], path.name)
    return meta


def extract_grammys(batch_id: str) -> dict:
    df = read_grammy_table()
    p = config.pg_params(config.SOURCE_DB)
    path = staging_dir(batch_id) / "grammy_raw.parquet"
    df.to_parquet(path, index=False)
    meta = {"batch_id": batch_id, "dataset": "grammy_raw", "path": str(path), "rows": int(len(df)),
            "source": f"PostgreSQL {p['dbname']}.public.grammy_awards", "query": GRAMMY_QUERY,
            "db_host": p["host"], "db_port": p["port"], "grammy_csv_read": False}
    _write_evidence(batch_id, "grammy_extract", meta)   # REQUIRED EVIDENCE: Grammy comes from the database
    log.info("[%s] extract_grammys: %s rows from %s (host=%s) -> %s", batch_id, meta["rows"], meta["source"],
             p["host"], path.name)
    return meta