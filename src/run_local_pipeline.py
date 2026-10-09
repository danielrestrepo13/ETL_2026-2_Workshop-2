"""Runs the full pipeline locally in the same order the DAG uses (no Airflow required).

    python -m src.run_local_pipeline

Pass --skip-load (or set env var SKIP_DW_LOAD=1) to stop after the prepared validation
gate and omit the DW write (useful when music_dw is not running locally).
"""
import json
import logging
import os
import sys

from src import extract, load, transform, validation

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
for noisy in ("great_expectations", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.WARNING)


def main():
    skip_load = "--skip-load" in sys.argv or os.environ.get("SKIP_DW_LOAD", "").lower() in ("1", "true", "yes")
    use_bad = "--bad" in sys.argv or "--scenario-b" in sys.argv

    from src import config
    if use_bad:
        bad_path = config.DATA_DIR / "raw" / "spotify_bad.csv"
        if not bad_path.exists():
            from src import controlled_failure
            controlled_failure.create_bad_dataset()
        csv_path = bad_path
    else:
        csv_path = config.SPOTIFY_CSV

    batch = extract.new_batch_id()
    sp, gr = extract.extract_spotify(batch, csv_path=csv_path), extract.extract_grammys(batch)
    sp, gr = validation.validate_raw(sp), validation.validate_raw(gr)
    prepared = transform.transform_and_integrate(sp, gr)
    prepared = validation.validate_prepared(prepared)

    result = {"batch_id": batch, "rows_prepared": prepared["rows"],
               "prepared_gate": prepared["validation_action"]}

    if skip_load:
        result["load"] = "skipped (--skip-load)"
    else:
        load_summary = load.load_to_dw(prepared)
        result["load"] = {"rows_loaded": load_summary["rows_loaded"],
                          "elapsed_s": load_summary["elapsed_s"]}

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()