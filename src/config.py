"""Shared configuration: project paths and PostgreSQL connection parameters.

Works in both places:
  * inside Docker (Airflow containers): host/port come from ANALYTICS_PG_HOST / ANALYTICS_PG_PORT
  * on your machine (scripts, notebooks): localhost + ANALYTICS_PG_HOST_PORT, read from .env
No credentials live in the code.
"""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent  # /opt/airflow in Docker
DATA_DIR = PROJECT_ROOT / "data"
SQL_DIR = PROJECT_ROOT / "sql"
EVIDENCE_DIR = PROJECT_ROOT / "docs" / "evidence"
STAGING_DIR = DATA_DIR / "staging"   # per-batch Parquet handoff between tasks (not committed)

SPOTIFY_CSV = DATA_DIR / "raw" / "spotify_dataset.csv"
GRAMMY_CSV = DATA_DIR / "raw" / "the_grammy_awards.csv"

SOURCE_DB = "grammy_source"  # operational Grammy source
DW_DB = "music_dw"           # analytical Data Warehouse


def _in_docker() -> bool:
    return Path("/.dockerenv").exists()


def _load_dotenv() -> None:
    """Minimal .env reader (no extra dependency). Real env vars win."""
    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.split(" #")[0].strip())


def pg_params(dbname: str) -> dict:
    """psycopg2-style connection parameters for one of the analytical databases."""
    if not _in_docker():
        _load_dotenv()
    if _in_docker():
        host = os.environ.get("ANALYTICS_PG_HOST", "analytics-postgres")
        port = int(os.environ.get("ANALYTICS_PG_PORT", "5432"))
    else:
        host = "localhost"
        port = int(os.environ.get("ANALYTICS_PG_HOST_PORT", "5433"))
    return {
        "host": host,
        "port": port,
        "user": os.environ.get("ANALYTICS_PG_USER", "etl_user"),
        "password": os.environ["ANALYTICS_PG_PASSWORD"],
        "dbname": dbname,
    }