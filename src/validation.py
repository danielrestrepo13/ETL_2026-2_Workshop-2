"""Quality rules (6.5) and Great Expectations execution (6.6).

Single source of truth: RULES (what must be true) and build_expectations() (how GX checks it).
Every expectation carries meta={"rule_id": ...} and the severity of its rule, so results map back to the rule.

Design (per dataset):  Data Asset -> Batch Definition -> Expectation Suite -> Validation Definition -> Checkpoint
Execution policy:      Critical failure -> STOP (DataQualityError) | Warning -> CONTINUE WITH WARNING | Info -> recorded
Results:               one JSON per dataset in docs/evidence/validation/<run_label>/
"""
import json
import logging
from datetime import datetime, timezone

import great_expectations as gx
import pandas as pd

from src import config

log = logging.getLogger(__name__)
E = gx.expectations

CRITICAL, WARNING, INFO = "critical", "warning", "info"
SEVERITY_LABEL = {CRITICAL: "Critical", WARNING: "Warning", INFO: "Informational"}


class DataQualityError(Exception):
    """A Critical rule failed: deterministic data failure, not retryable."""

    def __init__(self, message, summary=None):
        super().__init__(message)
        self.summary = summary


# ---------------------------------------------------------------------------------------------
# RULES: the quality-rule table (6.5)
# ---------------------------------------------------------------------------------------------
def _r(rid, layer, dataset, attrs, dim, rule, threshold, sev, req, risks, why, th_why):
    return dict(rule_id=rid, layer=layer, dataset=dataset, attributes=attrs, dimension=dim, rule=rule,
                threshold=threshold, severity=sev, requirement=req, risks=risks, justification=why,
                threshold_rationale=th_why)

RULES = {r["rule_id"]: r for r in [
    # ---- Spotify raw
    _r("DQ01", "raw", "spotify_raw", "required columns", "Validity (schema)",
       "All columns used downstream are present.", "100% of required columns", CRITICAL, "R1-R3", "RK01",
       "Source contract: transform and load read these columns.",
       "A missing column makes the transformation impossible, so no tolerance."),
    _r("DQ02", "raw", "spotify_raw", "track_id", "Completeness", "track_id is never null.", "100% non-null",
       CRITICAL, "R1-R3", "RK07", "Business key of dim_track (NOT NULL, UNIQUE); profiling found 0 nulls.",
       "It is a key: one null breaks the dimension load."),
    _r("DQ03", "raw", "spotify_raw", "popularity", "Validity", "popularity is between 0 and 100.",
       "100% within range", CRITICAL, "R1, R3", "RK15",
       "Profiling range 0-100; fact table CHECK constraint. Out-of-range values would silently distort KPI-1a and KPI-3b.",
       "Domain rule with a fixed definition: any violation is an error, not noise."),
    _r("DQ04", "raw", "spotify_raw", "danceability, energy, valence, acousticness", "Validity",
       "The four audio features used by R2 are between 0 and 1.", "100% within range", CRITICAL, "R2", "RK17",
       "Spotify defines them on [0,1]; fact table CHECK constraints; profiling found 0 violations.",
       "Fixed domain: a violation would break the load and the genre comparison."),
    _r("DQ05", "raw", "spotify_raw", "artists", "Completeness", "artists is not null.", ">= 99.9% non-null",
       WARNING, "R1-R3", "RK02",
       "A row without artist cannot be integrated (mapped to the unknown member). Profiling: 1 row.",
       "0.1% is about 114 rows: tolerates isolated gaps but flags a damaged export, where tracks would drop out of every artist KPI."),
    _r("DQ06", "raw", "spotify_raw", "(track_id, track_genre)", "Uniqueness", "A (track, genre) pair appears once.",
       ">= 99% unique pairs", WARNING, "R1-R3", "RK07",
       "Profiling: 450 exact duplicate rows (0.39%). The transformation removes them, so low duplication is a handled condition.",
       "1% is about 1,140 rows: above it the file was probably appended twice (a doubled file would show about 50%)."),
    _r("DQ07", "raw", "spotify_raw", "popularity", "Validity (monitoring)", "Most rows have popularity >= 1.",
       ">= 80% of rows with popularity >= 1", INFO, "R1, R3", "RK15",
       "Profiling: 14% of rows have popularity 0 and its meaning is not established; trend signal only.",
       "Alert when zeros exceed 1 in 5 rows, the point where zeros would dominate the averages of KPI-1a."),
    # ---- Grammy raw
    _r("DQ08", "raw", "grammy_raw", "required columns", "Validity (schema)",
       "All columns used downstream are present.", "100% of required columns", CRITICAL, "R3", "RK10",
       "Source contract of grammy_source.public.grammy_awards.", "A missing column makes the transformation impossible."),
    _r("DQ09", "raw", "grammy_raw", "source_row_id", "Uniqueness", "source_row_id is unique.", "100% unique",
       CRITICAL, "R3", "RK10", "Becomes award_id, part of the UNIQUE grain (award_id, artist_key) of fact_grammy_award.",
       "It is an identifier: duplicates would collapse or duplicate awards."),
    _r("DQ10", "raw", "grammy_raw", "year", "Validity", "Ceremony year is between 1950 and 2100.",
       "100% within range", CRITICAL, "R3", "RK19",
       "Matches the CHECK of dim_year; profiling 1958-2019 with no gaps.",
       "The range protects the load contract (it is deliberately wider than the current data)."),
    _r("DQ11", "raw", "grammy_raw", "category", "Completeness", "category is never null.", "100% non-null",
       CRITICAL, "R3", "RK12", "FK category_key is NOT NULL; profiling found 0 nulls.",
       "A null category cannot be loaded."),
    _r("DQ12", "raw", "grammy_raw", "winner", "Validity (source contract)", "winner is True in every row.",
       "100% True", CRITICAL, "R3", "RK11",
       "Profiling: all 4,810 rows are True, so the DW measure is award_count. A False row would count a non-win as an award.",
       "A different meaning of the table would invalidate R3; it requires a contract review, not a tolerance."),
    _r("DQ13", "raw", "grammy_raw", "artist", "Completeness", "artist is present.", ">= 50% non-null", WARNING,
       "R1-R3", "RK05", "Profiling: 38% of rows have no artist; those rows cannot be integrated with Spotify.",
       "Below 50% the artist-level KPIs would cover less than half of the awards."),
    # ---- Prepared
    _r("DQ14", "prepared", "prepared_dim_artist", "artist_match_key", "Uniqueness (duplicate match)",
       "One row per integration key.", "100% unique", CRITICAL, "R1-R3", "RK22",
       "Business key of dim_artist (UNIQUE). Duplicate keys would join one artist twice.", "Key constraint."),
    _r("DQ15", "prepared", "prepared_track_credit", "(track_id, artist_match_key, genre_name)", "Uniqueness",
       "Fact grain: one row per (track, artist, genre).", "100% unique", CRITICAL, "R1-R3", "RK07, RK14",
       "Declared grain of fact_track_credit; backed by a UNIQUE constraint.", "Grain violation inflates every count."),
    _r("DQ16", "prepared", "prepared_award_credit", "(award_id, artist_match_key)", "Uniqueness",
       "Fact grain: one row per (award entry, artist).", "100% unique", CRITICAL, "R3", "RK23",
       "Declared grain of fact_grammy_award; backed by a UNIQUE constraint.", "Grain violation inflates award counts."),
    _r("DQ17", "prepared", "all prepared datasets", "artist_match_key, artist_type", "Validity / Completeness",
       "Artist keys are never null (unknown and generic credits use their special member) and artist_type is REAL, PLACEHOLDER or UNKNOWN.",
       "100%", CRITICAL, "R1-R3", "RK05, RK25",
       "FK artist_key is NOT NULL and artist_type has a CHECK; approved decision: missing and generic artists map to special members.",
       "Referential contract of the DW."),
    _r("DQ18", "prepared", "prepared_track_credit", "popularity, danceability, energy, valence, acousticness",
       "Validity", "Measures are not null; popularity is within 0-100 and audio features within 0-1.",
       "100% valid", CRITICAL, "R1, R2", "RK15, RK17",
       "Post-transformation invariant: the transformation must not produce values outside the domain.",
       "The DW CHECK constraints would reject the load."),
    _r("DQ19", "prepared", "prepared_dim_artist", "grammy_spotify_overlap", "Integration readiness",
       "Enough artists present in both sources (Grammy awards and Spotify tracks) to compare against the rest.",
       ">= 30 artists in both sources", CRITICAL, "R1-R3", "RK22",
       "Analytical readiness: R1 and R2 compare Grammy and non-Grammy artists on Spotify measures, so only artists present in both sources give a Grammy group. Profiling found 540 matching keys before name splitting.",
       "30 is the usual minimum group size for comparing averages; below it the KPIs are not meaningful."),
    _r("DQ20", "prepared", "prepared_award_credit", "matched_in_spotify", "Integration integrity (unmatched)",
       "A reasonable share of credited real artists is found in Spotify.", ">= 33% matched", WARNING, "R3",
       "RK22, RK23",
       "Profiling: 47% of Grammy rows with artist matched before collaborations were split; splitting should not lower it.",
       "Below one third most awarded artists would show no Spotify presence in R3: coverage drops but data remains valid."),
]}

SPOTIFY_REQUIRED = ["track_id", "artists", "album_name", "track_name", "popularity", "duration_ms", "explicit",
                    "danceability", "energy", "valence", "acousticness", "track_genre"]
GRAMMY_REQUIRED = ["source_row_id", "title", "year", "category", "nominee", "artist", "winner"]
AUDIO = ["danceability", "energy", "valence", "acousticness"]
ARTIST_TYPES = ["REAL", "PLACEHOLDER", "UNKNOWN"]


def _x(cls, rid, **kwargs):
    return cls(severity=RULES[rid]["severity"], meta={"rule_id": rid}, **kwargs)


def build_expectations(dataset: str) -> list:
    if dataset == "spotify_raw":
        return [
            _x(E.ExpectTableColumnsToMatchSet, "DQ01", column_set=SPOTIFY_REQUIRED, exact_match=False),
            _x(E.ExpectColumnValuesToNotBeNull, "DQ02", column="track_id"),
            _x(E.ExpectColumnValuesToBeBetween, "DQ03", column="popularity", min_value=0, max_value=100),
            *[_x(E.ExpectColumnValuesToBeBetween, "DQ04", column=c, min_value=0, max_value=1) for c in AUDIO],
            _x(E.ExpectColumnValuesToNotBeNull, "DQ05", column="artists", mostly=0.999),
            _x(E.ExpectCompoundColumnsToBeUnique, "DQ06", column_list=["track_id", "track_genre"], mostly=0.99),
            _x(E.ExpectColumnValuesToBeBetween, "DQ07", column="popularity", min_value=1, mostly=0.80),
        ]
    if dataset == "grammy_raw":
        return [
            _x(E.ExpectTableColumnsToMatchSet, "DQ08", column_set=GRAMMY_REQUIRED, exact_match=False),
            _x(E.ExpectColumnValuesToBeUnique, "DQ09", column="source_row_id"),
            _x(E.ExpectColumnValuesToBeBetween, "DQ10", column="year", min_value=1950, max_value=2100),
            _x(E.ExpectColumnValuesToNotBeNull, "DQ11", column="category"),
            _x(E.ExpectColumnValuesToBeInSet, "DQ12", column="winner", value_set=[True]),
            _x(E.ExpectColumnValuesToNotBeNull, "DQ13", column="artist", mostly=0.50),
        ]
    if dataset == "prepared_dim_artist":
        return [
            _x(E.ExpectColumnValuesToBeUnique, "DQ14", column="artist_match_key"),
            _x(E.ExpectColumnValuesToNotBeNull, "DQ17", column="artist_match_key"),
            _x(E.ExpectColumnValuesToBeInSet, "DQ17", column="artist_type", value_set=ARTIST_TYPES),
            _x(E.ExpectColumnSumToBeBetween, "DQ19", column="grammy_spotify_overlap", min_value=30),
        ]
    if dataset == "prepared_track_credit":
        return [
            _x(E.ExpectCompoundColumnsToBeUnique, "DQ15", column_list=["track_id", "artist_match_key", "genre_name"]),
            _x(E.ExpectColumnValuesToNotBeNull, "DQ17", column="artist_match_key"),
            _x(E.ExpectColumnValuesToNotBeNull, "DQ18", column="popularity"),
            _x(E.ExpectColumnValuesToBeBetween, "DQ18", column="popularity", min_value=0, max_value=100),
            *[_x(E.ExpectColumnValuesToBeBetween, "DQ18", column=c, min_value=0, max_value=1) for c in AUDIO],
        ]
    if dataset == "prepared_award_credit":
        return [
            _x(E.ExpectCompoundColumnsToBeUnique, "DQ16", column_list=["award_id", "artist_match_key"]),
            _x(E.ExpectColumnValuesToNotBeNull, "DQ17", column="artist_match_key"),
            _x(E.ExpectColumnMeanToBeBetween, "DQ20", column="matched_in_spotify", min_value=0.33),
        ]
    raise KeyError(dataset)


DATASETS = {"raw": ["spotify_raw", "grammy_raw"],
            "prepared": ["prepared_dim_artist", "prepared_track_credit", "prepared_award_credit"]}


# ---------------------------------------------------------------------------------------------
# GX stack and execution (6.6)
# ---------------------------------------------------------------------------------------------
def build_stack(dataset: str):
    """Ephemeral context per execution (no shared state between parallel Airflow tasks).
    Returns (suite, validation_definition, checkpoint)."""
    context = gx.get_context(mode="ephemeral")
    try:
        from great_expectations.data_context.types.base import ProgressBarsConfig
        context.variables.progress_bars = ProgressBarsConfig(globally=False)
    except Exception:  # cosmetic only
        pass
    source = context.data_sources.add_pandas(name=f"{dataset}_pandas_source")
    asset = source.add_dataframe_asset(name=dataset)
    batch_def = asset.add_batch_definition_whole_dataframe(name=f"whole_{dataset}")
    suite = context.suites.add(gx.ExpectationSuite(name=f"{dataset}_suite"))
    for exp in build_expectations(dataset):
        suite.add_expectation(exp)
    vdef = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"{dataset}_validation", data=batch_def, suite=suite))
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(name=f"{dataset}_checkpoint", validation_definitions=[vdef], actions=[],
                      result_format={"result_format": "SUMMARY"}))
    return suite, vdef, checkpoint


def _sev(result) -> str:
    return str(result.expectation.severity).lower().split(".")[-1]


def validate_dataset(dataset: str, df: pd.DataFrame, run_label: str, raise_on_critical: bool = True) -> dict:
    """Run the checkpoint of one dataset, persist evidence, apply the severity policy."""
    _, _, checkpoint = build_stack(dataset)
    ckpt_result = checkpoint.run(batch_parameters={"dataframe": df})

    checks = []
    for run in ckpt_result.run_results.values():
        for res in run.results:
            exp = res.expectation
            checks.append({
                "rule_id": (exp.meta or {}).get("rule_id"),
                "expectation": type(exp).__name__,
                "target": getattr(exp, "column", None) or getattr(exp, "column_list", None) or "table",
                "severity": _sev(res),
                "success": bool(res.success),
                "unexpected_count": res.result.get("unexpected_count"),
                "unexpected_percent": res.result.get("unexpected_percent"),
                "observed_value": (None if res.result.get("observed_value") is None
                                   else str(res.result.get("observed_value"))[:200]),
            })
    checks.sort(key=lambda c: (c["rule_id"] or "", str(c["target"])))
    failed = [c for c in checks if not c["success"]]
    n_crit = sum(c["severity"] == CRITICAL for c in failed)
    n_warn = sum(c["severity"] == WARNING for c in failed)
    n_info = sum(c["severity"] == INFO for c in failed)
    action = "STOP" if n_crit else ("CONTINUE WITH WARNING" if n_warn else "CONTINUE")

    summary = {
        "run_label": run_label, "dataset": dataset, "validated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": int(len(df)), "overall_success": bool(ckpt_result.success), "pipeline_action": action,
        "failed_critical": n_crit, "failed_warning": n_warn, "failed_info": n_info, "checks": checks,
    }
    out_dir = config.EVIDENCE_DIR / "validation" / run_label
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{dataset}.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    (out_dir / f"{dataset}_gx_full.json").write_text(
        json.dumps(ckpt_result.describe_dict(), indent=2, default=str), encoding="utf-8")

    log.info("[%s] %s rows=%s action=%s critical=%s warning=%s info=%s", run_label, dataset, len(df), action,
             n_crit, n_warn, n_info)
    for c in failed:
        log.log(logging.ERROR if c["severity"] == CRITICAL else logging.WARNING,
                "[%s] %s FAILED %s on %s (severity=%s, unexpected=%s, %s%%)", dataset, c["rule_id"], c["expectation"],
                c["target"], c["severity"], c["unexpected_count"], c["unexpected_percent"])
    if n_crit and raise_on_critical:
        ids = sorted({c["rule_id"] for c in failed if c["severity"] == CRITICAL})
        raise DataQualityError(f"{dataset}: Critical rule(s) failed {ids}. Downstream processing blocked.", summary)
    return summary


# ---------------------------------------------------------------------------------------------
# Task-level gates: read staged Parquet, validate, apply policy
# ---------------------------------------------------------------------------------------------
def validate_raw(meta: dict) -> dict:
    """Raw gate for one source. Raises DataQualityError on a Critical failure (blocks downstream)."""
    df = pd.read_parquet(meta["path"])
    summary = validate_dataset(meta["dataset"], df, run_label=meta["batch_id"], raise_on_critical=True)
    return {**meta, "validation_action": summary["pipeline_action"]}


def validate_prepared(prepared_meta: dict) -> dict:
    """Prepared gate over the three validated datasets. All run first, then Critical failures block the load."""
    summaries = []
    for name in DATASETS["prepared"]:
        df = pd.read_parquet(prepared_meta["paths"][name])
        summaries.append(validate_dataset(name, df, run_label=prepared_meta["batch_id"], raise_on_critical=False))
    crit = [s for s in summaries if s["failed_critical"]]
    if crit:
        ids = sorted({c["rule_id"] for s in crit for c in s["checks"] if not c["success"] and c["severity"] == CRITICAL})
        raise DataQualityError(f"Prepared validation: Critical rule(s) failed {ids}. Load blocked.", crit)
    action = "CONTINUE WITH WARNING" if any(s["failed_warning"] for s in summaries) else "CONTINUE"
    return {**prepared_meta, "validation_action": action}


# ---------------------------------------------------------------------------------------------
# Documentation export (generated from RULES and the suites, so docs cannot drift from code)
# ---------------------------------------------------------------------------------------------
def _md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(v).replace("|", "/") for v in r) + " |" for r in rows]
    return "\n".join(out)


def export_documentation():
    rules = list(RULES.values())
    t_rules = _md_table(
        ["Rule ID", "Dataset / Layer", "Attribute(s)", "Quality Dimension", "Quality Rule", "Metric / Threshold", "Severity", "Related Requirement"],
        [[r["rule_id"], f'{r["dataset"]} ({r["layer"]})', r["attributes"], r["dimension"], r["rule"], r["threshold"],
          SEVERITY_LABEL[r["severity"]], r["requirement"]] for r in rules])
    t_why = _md_table(
        ["Rule ID", "Profiling risk", "Justification (evidence / requirement / contract / invariant)", "Why this threshold"],
        [[r["rule_id"], r["risks"], r["justification"], r["threshold_rationale"]] for r in rules])

    map_rows, expectation_rows = [], []
    for layer in ("raw", "prepared"):
        for ds in DATASETS[layer]:
            suite, vdef, ckpt = build_stack(ds)
            (config.PROJECT_ROOT / "gx" / "expectations").mkdir(parents=True, exist_ok=True)
            (config.PROJECT_ROOT / "gx" / "expectations" / f"{ds}_suite.json").write_text(
                json.dumps(suite.to_json_dict(), indent=2, default=str), encoding="utf-8")
            for exp in build_expectations(ds):
                kw = exp.configuration.kwargs
                target = kw.get("column") or kw.get("column_list") or "table"
                params = {k: v for k, v in kw.items() if k not in ("column", "column_list", "batch_id")}
                map_rows.append([RULES[exp.meta["rule_id"]]["rule_id"], ds, type(exp).__name__, target, params,
                                 SEVERITY_LABEL[RULES[exp.meta['rule_id']]['severity']]])
            expectation_rows.append([ds, f"{ds}_suite", f"{ds}_validation", f"{ds}_checkpoint", layer])
    t_map = _md_table(["Rule ID", "Suite (dataset)", "GX Expectation", "Target", "Parameters", "Severity"], map_rows)
    t_gx = _md_table(["Data object", "Expectation Suite", "Validation Definition", "Checkpoint", "Gate"], expectation_rows)

    ev = config.EVIDENCE_DIR / "validation"
    ev.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(map_rows, columns=["rule_id", "suite", "expectation", "target", "parameters", "severity"]
                 ).to_csv(ev / "expectation_rule_map.csv", index=False)

    doc = f"""# Data Quality Rules and Great Expectations Design (6.5 / 6.6)

Generated by `python -m src.validation` from `RULES` and the suites in `src/validation.py`; do not edit by hand.
Risk IDs (`RKxx`) come from `docs/evidence/profiling/risk_register.md`.

## 1. Quality rules

{t_rules}

## 2. Justification and threshold rationale

A threshold is an engineering decision. Each one below is tied to the protected requirement or to the DW contract,
not to the fact that the current batch passes.

{t_why}

## 3. Severity policy and pipeline response

| Severity | Pipeline response | Retry |
|---|---|---|
| Critical | **STOP**: `DataQualityError` fails the validation task; downstream tasks are not run (transform, prepared validation, load). | No: a deterministic data failure reproduces itself. |
| Warning | **CONTINUE WITH WARNING**: logged, stored in the result JSON, shown in the task log. | n/a |
| Informational | Recorded for trend monitoring; never blocks. | n/a |

## 4. Great Expectations design

Each dataset follows the same chain, built in `build_stack()` with an ephemeral context per execution (no shared state, so
the two raw branches can run in parallel in Airflow):

`DataFrame asset -> Batch Definition (whole dataframe) -> Expectation Suite -> Validation Definition -> Checkpoint`

{t_gx}

Controlled execution: `validate_dataset()` runs the checkpoint, summarizes every expectation (rule, severity, unexpected
count and percent), writes the evidence and applies the severity policy. Suites are also exported to `gx/expectations/`.

Results are kept in `docs/evidence/validation/<run_label>/`: `<dataset>.json` (per-rule summary and pipeline action) and
`<dataset>_gx_full.json` (complete Great Expectations result).

## 5. Expectation to Rule ID map

{t_map}
"""
    (config.PROJECT_ROOT / "docs" / "quality_rules.md").write_text(doc, encoding="utf-8")
    return config.PROJECT_ROOT / "docs" / "quality_rules.md"


if __name__ == "__main__":
    print("Written:", export_documentation())