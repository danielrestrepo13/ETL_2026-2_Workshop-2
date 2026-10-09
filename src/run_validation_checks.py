"""Runs the raw validation gates on the real sources and three controlled scenarios (evidence for 6.6 / 6.7).

    python -m src.run_validation_checks

  A. baseline        both raw sources as they are             -> expected: CONTINUE (no Critical failure)
  B. critical        popularity out of range / winner=False   -> expected: STOP (DQ03, DQ12)
  C. warning         3% of Spotify rows without artist        -> expected: CONTINUE WITH WARNING (DQ05)

Scenarios B and C corrupt in-memory copies only; sources and database are never modified.
"""
import logging
import sys

import pandas as pd

from src import extract, validation

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def run(label, spotify, grammy):
    print(f"\n=== Scenario {label} ===")
    results = []
    for name, df in (("spotify_raw", spotify), ("grammy_raw", grammy)):
        try:
            s = validation.validate_dataset(name, df, run_label=label, raise_on_critical=True)
            results.append((name, s["pipeline_action"], "-"))
        except validation.DataQualityError as exc:
            failed = sorted({c["rule_id"] for c in exc.summary["checks"] if not c["success"] and c["severity"] == "critical"})
            results.append((name, "STOP", ", ".join(failed)))
    print(pd.DataFrame(results, columns=["dataset", "pipeline_action", "critical_rules_failed"]).to_string(index=False))
    return results


def main():
    spotify, grammy = extract.read_spotify_csv(), extract.read_grammy_table()

    run("A_baseline", spotify, grammy)

    bad_s = spotify.copy()
    bad_s.loc[bad_s.index[:100], "popularity"] = 150           # DQ03
    bad_g = grammy.copy()
    bad_g.loc[bad_g.index[:50], "winner"] = False              # DQ12
    run("B_critical_failure", bad_s, bad_g)

    warn_s = spotify.copy()
    warn_s.loc[warn_s.sample(frac=0.03, random_state=1).index, "artists"] = None   # DQ05
    run("C_warning_only", warn_s, grammy)


if __name__ == "__main__":
    sys.exit(main())