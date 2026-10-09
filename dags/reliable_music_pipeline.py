"""
Reliable Music Pipeline - Workshop 2
Building a Reliable Batch Data Pipeline

Course: ETL - Data Engineering and Artificial Intelligence
Airflow: 3.1.8
Great Expectations: 1.23.x

Analytical requirements:
R1 - Compare Spotify audio features for Grammy-winning vs non-winning artists.
R2 - Characterize Grammy award patterns across genres, categories, and decades.
R3 - Build an integrated artist profile crossing Grammy recognition with Spotify popularity.

The DAG exchanges compact METADATA DICTS between tasks, not complete DataFrames.
All tasks read/write Parquet files under the staging directory, which must be mounted
as a shared Docker volume.
"""

from datetime import timedelta
from pathlib import Path

import great_expectations as gx
import pandas as pd
import pendulum

from airflow.sdk import dag, task
from great_expectations.expectations.metadata_types import FailureSeverity


# ============================================================
# GLOBAL CONFIGURATION / SOURCE SELECTION
# ============================================================
# Successful run (Baseline):
SPOTIFY_FILENAME = "spotify_dataset.csv"


# Controlled-failure run (Test B):
# After running `python -m src.controlled_failure`, change the previous line to:
# SPOTIFY_FILENAME = "spotify_bad.csv"


# ============================================================
# HELPER: BUILD AND RUN A GX VALIDATION
# ============================================================

def run_gx_validation(dataframe, stage):
    """
    Build a small ephemeral GX validation for one pipeline stage.

    stage = "spotify_raw"     Validates incoming Spotify CSV data.
    stage = "grammy_raw"      Validates incoming Grammy source data.
    stage = "prepared"        Validates the integrated analytical datasets.

    The GX context is ephemeral: each task run builds its own context with
    no shared state, which allows the two raw-validation tasks to run in
    parallel safely.
    """

    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_pandas(
        name=f"{stage}_pandas_source"
    )

    data_asset = data_source.add_dataframe_asset(
        name=f"{stage}_asset"
    )

    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f"whole_{stage}_batch"
    )

    suite = gx.ExpectationSuite(
        name=f"{stage}_suite"
    )
    suite = context.suites.add(suite)

    # ----------------------------------------------------------
    # SPOTIFY RAW — DQ01-DQ07
    # ----------------------------------------------------------
    if stage == "spotify_raw":

        # DQ01 — Required columns must be present.
        # Critical: transform and load read these columns.
        suite.add_expectation(
            gx.expectations.ExpectTableColumnsToMatchSet(
                column_set=[
                    "track_id", "artists", "album_name", "track_name",
                    "popularity", "duration_ms", "explicit",
                    "danceability", "energy", "valence", "acousticness",
                    "track_genre",
                ],
                exact_match=False,
                severity="critical",
            )
        )

        # DQ02 — track_id is never null.
        # Critical: business key of dim_track.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="track_id",
                severity="critical",
            )
        )

        # DQ03 — popularity is between 0 and 100.
        # Critical: domain rule and fact table CHECK constraint.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="popularity",
                min_value=0,
                max_value=100,
                severity="critical",
            )
        )

        # DQ04 — Audio features are between 0 and 1.
        # Critical: Spotify domain definition and fact table CHECK constraints.
        for audio_col in ["danceability", "energy", "valence", "acousticness"]:
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column=audio_col,
                    min_value=0,
                    max_value=1,
                    severity="critical",
                )
            )

        # DQ05 — artists is not null in at least 99.9% of rows.
        # Warning: rows without artist map to the unknown member.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="artists",
                mostly=0.999,
                severity="warning",
            )
        )

        # DQ06 — (track_id, track_genre) pairs are at least 99% unique.
        # Warning: exact duplicates are removed in transform.
        suite.add_expectation(
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["track_id", "track_genre"],
                mostly=0.99,
                severity="warning",
            )
        )

        # DQ07 — At least 80% of rows have popularity >= 1.
        # Informational: trend monitoring for zero-popularity inflation.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="popularity",
                min_value=1,
                mostly=0.80,
                severity="info",
            )
        )

    # ----------------------------------------------------------
    # GRAMMY RAW — DQ08-DQ13
    # ----------------------------------------------------------
    elif stage == "grammy_raw":

        # DQ08 — Required columns must be present.
        # Critical: source contract of grammy_source.public.grammy_awards.
        suite.add_expectation(
            gx.expectations.ExpectTableColumnsToMatchSet(
                column_set=[
                    "source_row_id", "title", "year", "category",
                    "nominee", "artist", "winner",
                ],
                exact_match=False,
                severity="critical",
            )
        )

        # DQ09 — source_row_id is unique.
        # Critical: becomes award_id, part of the fact grain.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeUnique(
                column="source_row_id",
                severity="critical",
            )
        )

        # DQ10 — year is between 1950 and 2100.
        # Critical: matches the CHECK constraint of dim_year.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="year",
                min_value=1950,
                max_value=2100,
                severity="critical",
            )
        )

        # DQ11 — category is never null.
        # Critical: FK category_key is NOT NULL.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="category",
                severity="critical",
            )
        )

        # DQ12 — winner is True in every row.
        # Critical: source contract; a False row would count a non-win.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="winner",
                value_set=[True],
                severity="critical",
            )
        )

        # DQ13 — artist is present in at least 50% of rows.
        # Warning: 38% missing in source; rows below threshold lose artist KPIs.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="artist",
                mostly=0.50,
                severity="warning",
            )
        )

    # ----------------------------------------------------------
    # PREPARED — DQ14-DQ20 (three datasets validated sequentially)
    # ----------------------------------------------------------
    elif stage == "prepared_dim_artist":

        # DQ14 — artist_match_key is unique.
        # Critical: business key of dim_artist; duplicates join an artist twice.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeUnique(
                column="artist_match_key",
                severity="critical",
            )
        )

        # DQ17 — artist_match_key is never null and artist_type is valid.
        # Critical: FK artist_key is NOT NULL and artist_type has a CHECK.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="artist_match_key",
                severity="critical",
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="artist_type",
                value_set=["REAL", "PLACEHOLDER", "UNKNOWN"],
                severity="critical",
            )
        )

        # DQ19 — At least 30 artists are present in both sources.
        # Critical: analytical readiness for R1 and R2.
        suite.add_expectation(
            gx.expectations.ExpectColumnSumToBeBetween(
                column="grammy_spotify_overlap",
                min_value=30,
                severity="critical",
            )
        )

    elif stage == "prepared_track_credit":

        # DQ15 — (track_id, artist_match_key, genre_name) is the declared grain.
        # Critical: backed by a UNIQUE constraint; grain violation inflates counts.
        suite.add_expectation(
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["track_id", "artist_match_key", "genre_name"],
                severity="critical",
            )
        )

        # DQ17 — artist_match_key is never null.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="artist_match_key",
                severity="critical",
            )
        )

        # DQ18 — Measures are not null and within domain.
        # Critical: DW CHECK constraints would reject the load.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="popularity",
                severity="critical",
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="popularity",
                min_value=0,
                max_value=100,
                severity="critical",
            )
        )
        for audio_col in ["danceability", "energy", "valence", "acousticness"]:
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column=audio_col,
                    min_value=0,
                    max_value=1,
                    severity="critical",
                )
            )

    elif stage == "prepared_award_credit":

        # DQ16 — (award_id, artist_match_key) is the declared grain.
        # Critical: backed by a UNIQUE constraint; grain violation inflates award counts.
        suite.add_expectation(
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["award_id", "artist_match_key"],
                severity="critical",
            )
        )

        # DQ17 — artist_match_key is never null.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="artist_match_key",
                severity="critical",
            )
        )

        # DQ20 — At least 33% of real credited artists matched in Spotify.
        # Warning: below one third most awarded artists show no Spotify presence.
        suite.add_expectation(
            gx.expectations.ExpectColumnMeanToBeBetween(
                column="matched_in_spotify",
                min_value=0.33,
                severity="warning",
            )
        )

    else:
        raise ValueError(f"Unknown validation stage: {stage}")

    validation_definition = gx.ValidationDefinition(
        name=f"{stage}_validation",
        data=batch_definition,
        suite=suite,
    )

    validation_definition = context.validation_definitions.add(
        validation_definition
    )

    result = validation_definition.run(
        batch_parameters={"dataframe": dataframe},
        result_format={"result_format": "SUMMARY"},
    )

    print(f"GX stage: {stage}")
    print(f"Validation success: {result.success}")
    print(f"Statistics: {result.statistics}")

    max_failure = result.get_max_severity_failure()
    print(f"Maximum failure severity: {max_failure}")

    return result, max_failure


# ============================================================
# DAG DEFINITION
# ============================================================

@dag(
    dag_id="reliable_music_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "gx", "reliable-pipeline", "workshop2", "music_dw"],
)
def reliable_music_pipeline():

    # ========================================================
    # STAGE 1 - EXTRACT  (two parallel tasks)
    # ========================================================

    @task
    def extract_spotify():
        """
        Read the Spotify CSV, stage it as Parquet and return a compact
        metadata dict.  Bulk data never travels through Airflow.
        """
        from src import config as _config
        from src import extract as _extract

        source_path = _config.DATA_DIR / "raw" / SPOTIFY_FILENAME
        batch_id = _extract.new_batch_id()
        meta = _extract.extract_spotify(batch_id, csv_path=source_path)

        print(f"Batch: {batch_id}")
        print(f"Source file: {source_path}")
        print(f"Rows extracted: {meta['rows']}")
        print(f"Staged at: {meta['path']}")

        return meta

    @task(
        retries=2,
        retry_delay=timedelta(minutes=3),
    )
    def extract_grammys(spotify_meta):
        """
        Extract Grammy awards from the relational source database and stage
        as Parquet.  Receives spotify_meta only to reuse the same batch_id;
        no bulk data travels through XCom.

        retries=2: transient database or network failure.
        """
        from src import extract as _extract

        meta = _extract.extract_grammys(spotify_meta["batch_id"])

        print(f"Batch: {meta['batch_id']}")
        print(f"Rows extracted: {meta['rows']}")
        print(f"Staged at: {meta['path']}")

        return meta

    # ========================================================
    # STAGE 2 - RAW VALIDATION GATES  (two parallel tasks)
    # ========================================================

    @task
    def validate_raw_spotify(spotify_meta):
        """
        Validate Spotify raw data with Great Expectations BEFORE transformation.

        Critical GX failures stop the pipeline.
        Warning-level failures are logged but do not block execution.
        """

        df = pd.read_parquet(spotify_meta["path"])

        result, max_failure = run_gx_validation(
            dataframe=df,
            stage="spotify_raw",
        )

        if max_failure == FailureSeverity.CRITICAL:
            raise ValueError(
                "Spotify raw validation failed with CRITICAL severity. "
                "Transformation is blocked."
            )

        if max_failure == FailureSeverity.WARNING:
            print(
                "Spotify raw validation contains WARNING-level failures. "
                "The documented policy allows the pipeline to continue."
            )

        print("Spotify raw validation gate passed according to policy.")
        return {**spotify_meta, "validation_action": "CONTINUE" if max_failure is None else "CONTINUE WITH WARNING"}

    @task
    def validate_raw_grammys(grammy_meta):
        """
        Validate Grammy raw data with Great Expectations BEFORE transformation.

        Critical GX failures stop the pipeline.
        Warning-level failures are logged but do not block execution.
        """

        df = pd.read_parquet(grammy_meta["path"])

        result, max_failure = run_gx_validation(
            dataframe=df,
            stage="grammy_raw",
        )

        if max_failure == FailureSeverity.CRITICAL:
            raise ValueError(
                "Grammy raw validation failed with CRITICAL severity. "
                "Transformation is blocked."
            )

        if max_failure == FailureSeverity.WARNING:
            print(
                "Grammy raw validation contains WARNING-level failures. "
                "The documented policy allows the pipeline to continue."
            )

        print("Grammy raw validation gate passed according to policy.")
        return {**grammy_meta, "validation_action": "CONTINUE" if max_failure is None else "CONTINUE WITH WARNING"}

    # ========================================================
    # STAGE 3 - TRANSFORM AND INTEGRATE
    # ========================================================

    @task
    def transform_and_integrate(spotify_meta, grammy_meta):
        """
        Clean, standardize, split, integrate and reconcile Spotify and Grammy data.

        Raises ReconciliationError if any integration invariant fails (deterministic;
        retrying would produce the same failure).
        Returns a prepared_meta dict with paths to the four prepared Parquet files.
        """
        from src import transform as _transform

        prepared = _transform.transform_and_integrate(spotify_meta, grammy_meta)

        print(f"Batch: {prepared['batch_id']}")
        print(f"Rows prepared: {prepared['rows']}")
        print(f"Reconciliation report: {prepared['reconciliation']}")

        return prepared

    # ========================================================
    # STAGE 4 - PREPARED VALIDATION GATE
    # ========================================================

    @task
    def validate_prepared(prepared_meta):
        """
        Validate the three prepared datasets with Great Expectations BEFORE loading.

        All suites run first; then Critical failures block the load.
        Warning-level failures are logged but allow loading to proceed.
        """

        paths = prepared_meta["paths"]
        batch_id = prepared_meta["batch_id"]

        prepared_datasets = [
            ("prepared_dim_artist",   paths["prepared_dim_artist"]),
            ("prepared_track_credit", paths["prepared_track_credit"]),
            ("prepared_award_credit", paths["prepared_award_credit"]),
        ]

        critical_failures = []
        warning_failures = []

        for stage, parquet_path in prepared_datasets:
            df = pd.read_parquet(parquet_path)

            result, max_failure = run_gx_validation(
                dataframe=df,
                stage=stage,
            )

            if max_failure == FailureSeverity.CRITICAL:
                critical_failures.append(stage)

            elif max_failure == FailureSeverity.WARNING:
                warning_failures.append(stage)

        if critical_failures:
            raise ValueError(
                f"Prepared validation failed with CRITICAL severity in: "
                f"{critical_failures}. Loading is blocked."
            )

        if warning_failures:
            print(
                f"Prepared validation contains WARNING-level failures in: "
                f"{warning_failures}. "
                "The documented policy allows the load to proceed."
            )

        print("Prepared validation gate passed according to policy.")

        action = "CONTINUE WITH WARNING" if warning_failures else "CONTINUE"
        return {**prepared_meta, "validation_action": action}

    # ========================================================
    # STAGE 5 - LOAD TO DATA WAREHOUSE
    # ========================================================

    @task(
        retries=2,
        retry_delay=timedelta(minutes=3),
    )
    def load_dw(prepared_meta):
        """
        UPSERT dimensions and Truncate-and-Load facts into music_dw (PostgreSQL).

        The entire load runs inside a single transaction: a failed run leaves the
        previous DW content untouched (Safe Rerun guarantee).

        retries=2: transient database or network failure.
        """
        from src import load as _load

        summary = _load.load_to_dw(prepared_meta)

        print(f"Batch: {summary['batch_id']}")
        print(f"Loaded at: {summary['loaded_at_utc']}")
        print(f"Elapsed: {summary['elapsed_s']}s")
        print(f"Rows loaded: {summary['rows_loaded']}")

        return summary

    # ========================================================
    # BUILD THE WORKFLOW
    # ========================================================

    spotify_task = extract_spotify()
    grammy_task  = extract_grammys(spotify_task)

    validate_spotify_task = validate_raw_spotify(spotify_task)
    validate_grammy_task  = validate_raw_grammys(grammy_task)

    transform_task = transform_and_integrate(
        validate_spotify_task,
        validate_grammy_task,
    )

    # Validation is a gate: loading may only proceed after
    # prepared validation satisfies the documented policy.
    prepared_validation_task = validate_prepared(transform_task)

    load_dw(prepared_validation_task)


# Make the DAG discoverable by Airflow.
reliable_music_pipeline()
