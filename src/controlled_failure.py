"""Controlled failure generator for Test B (6.6 / 6.7).

Follows the exact academic practice pattern from airflow-docker-2026-2/src/controlled_failure.py.

Injects deterministic violations into DQ03 (popularity > 100) without modifying the original dataset.
When this corrupted dataset is processed by the pipeline or Airflow:
  - extract_spotify reads spotify_bad.csv and stages it.
  - validate_raw_spotify evaluates DQ03 and detects the Critical failure.
  - The pipeline stops immediately (STOP), turning the task RED in Airflow and
    blocking transform_and_integrate and load_dw.

Usage:
    python -m src.controlled_failure
"""
from pathlib import Path
import pandas as pd

from src import config

SOURCE_CSV = config.SPOTIFY_CSV
BAD_CSV = config.DATA_DIR / "raw" / "spotify_bad.csv"


def create_bad_dataset() -> Path:
    print(f"Reading original dataset from {SOURCE_CSV}...")
    df = pd.read_csv(SOURCE_CSV)

    # Inject deterministic critical violations (DQ03: popularity must be between 0 and 100)
    df_bad = df.copy()
    corrupted_count = 100
    df_bad.loc[df_bad.index[:corrupted_count], "popularity"] = 150

    df_bad.to_csv(BAD_CSV, index=False)
    print(f"Controlled failure dataset created at: {BAD_CSV}")
    print(f"Corrupted {corrupted_count} rows with popularity = 150 (violating DQ03, critical).")
    return BAD_CSV


if __name__ == "__main__":
    create_bad_dataset()
