"""Data Warehouse load (6.10). Reads validated prepared Parquet files and inserts them into music_dw.

Load strategies (idempotent / safe to rerun):
  - Dimensions (dim_artist, dim_genre, dim_track, dim_category, dim_year):
        UPSERT — INSERT ... ON CONFLICT (business_key) DO UPDATE SET ...
        Surrogate keys are stable once assigned; repeated runs update attributes in place.
  - Fact tables (fact_track_credit, fact_grammy_award):
        Truncate-and-Load inside a single BEGIN/TRUNCATE/INSERT/COMMIT block.
        The UNIQUE grain constraint prevents duplicates; the transaction guarantees that
        a partial failure leaves the previous content untouched.

Evidence: one JSON per batch in docs/evidence/load/<batch_id>/load_summary.json
"""
import json
import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

from src import config

log = logging.getLogger(__name__)


def _as_py(value):
    """Coerce a NumPy / pandas scalar to a native Python type that psycopg2 can adapt.

    psycopg2 does not natively handle numpy.int64, numpy.float64, numpy.bool_,
    or pandas NA/NaT.  This helper is applied to every value before it enters a
    parameterised INSERT row, so no global adapter registration is needed.
    """
    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    try:
        # pandas NA / NaT
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.generic):        # catch-all for other numpy scalars
        return value.item()
    return value


# ---- column lists that travel from the prepared datasets into each DW table ----
_DIM_ARTIST_COLS   = ["artist_match_key", "artist_name", "artist_type",
                       "has_spotify_tracks", "has_grammy_awards"]
_DIM_TRACK_COLS    = ["track_id", "track_name", "album_name", "explicit", "duration_ms"]
_FACT_TC_MEASURES  = ["popularity", "danceability", "energy", "valence", "acousticness"]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _conn(dbname: str):
    """Open a psycopg2 connection to one of the project's PostgreSQL databases."""
    return psycopg2.connect(**config.pg_params(dbname))


def _execute_many(cur, sql: str, rows: list[tuple]) -> int:
    """Bulk-execute a parameterised statement; returns rows affected."""
    psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    return cur.rowcount


def _write_evidence(batch_id: str, payload: dict) -> None:
    d = config.EVIDENCE_DIR / "load" / batch_id
    d.mkdir(parents=True, exist_ok=True)
    (d / "load_summary.json").write_text(
        json.dumps(payload, indent=2, default=str), encoding="utf-8")


# ---------------------------------------------------------------------------
# Dimension loaders  (UPSERT strategy)
# ---------------------------------------------------------------------------

def _load_dim_artist(cur, df: pd.DataFrame) -> int:
    """UPSERT dim_artist on business key artist_match_key.

    Special members (__unknown__, __placeholder__) are pre-seeded by dw_schema.sql
    (with surrogate keys -1 and -2).  We skip them here so the UPSERT never
    reassigns their fixed keys.
    """
    rows = df[~df["artist_match_key"].isin(["__unknown__", "__placeholder__"])][
        _DIM_ARTIST_COLS].copy()
    # has_spotify_tracks / has_grammy_awards may arrive as int (0/1) from Parquet; cast to bool
    rows["has_spotify_tracks"] = rows["has_spotify_tracks"].astype(bool)
    rows["has_grammy_awards"]  = rows["has_grammy_awards"].astype(bool)
    sql = """
        INSERT INTO dw.dim_artist (artist_match_key, artist_name, artist_type,
                                   has_spotify_tracks, has_grammy_awards)
        VALUES %s
        ON CONFLICT (artist_match_key) DO UPDATE SET
            artist_name        = EXCLUDED.artist_name,
            artist_type        = EXCLUDED.artist_type,
            has_spotify_tracks = EXCLUDED.has_spotify_tracks,
            has_grammy_awards  = EXCLUDED.has_grammy_awards
    """
    _execute_many(cur, sql, [tuple(_as_py(v) for v in r) for r in rows.itertuples(index=False)])
    return len(rows)


def _load_dim_genre(cur, genre_names: pd.Series) -> dict[str, int]:
    """UPSERT dim_genre; return {genre_name: genre_key} lookup."""
    sql = """
        INSERT INTO dw.dim_genre (genre_name)
        VALUES %s
        ON CONFLICT (genre_name) DO UPDATE SET genre_name = EXCLUDED.genre_name
        RETURNING genre_key, genre_name
    """
    names = [(n,) for n in sorted(genre_names.dropna().unique())]
    psycopg2.extras.execute_values(cur, sql, names, page_size=200)
    # Fetch the full map (including rows that already existed and returned nothing)
    cur.execute("SELECT genre_key, genre_name FROM dw.dim_genre")
    return {row[1]: row[0] for row in cur.fetchall()}


def _load_dim_track(cur, df: pd.DataFrame) -> dict[str, int]:
    """UPSERT dim_track on business key track_id; return {track_id: track_key} lookup."""
    rows = df[_DIM_TRACK_COLS].copy()
    # explicit and duration_ms may be nullable; _as_py converts numpy NA/NaN -> None
    sql = """
        INSERT INTO dw.dim_track (track_id, track_name, album_name, explicit, duration_ms)
        VALUES %s
        ON CONFLICT (track_id) DO UPDATE SET
            track_name  = EXCLUDED.track_name,
            album_name  = EXCLUDED.album_name,
            explicit    = EXCLUDED.explicit,
            duration_ms = EXCLUDED.duration_ms
        RETURNING track_key, track_id
    """
    psycopg2.extras.execute_values(
        cur, sql,
        [tuple(_as_py(v) for v in r) for r in rows.itertuples(index=False)],
        page_size=500)
    cur.execute("SELECT track_key, track_id FROM dw.dim_track")
    return {row[1]: row[0] for row in cur.fetchall()}


def _load_dim_category(cur, category_names: pd.Series) -> dict[str, int]:
    """UPSERT dim_category; return {category_name: category_key} lookup."""
    sql = """
        INSERT INTO dw.dim_category (category_name)
        VALUES %s
        ON CONFLICT (category_name) DO UPDATE SET category_name = EXCLUDED.category_name
    """
    names = [(n,) for n in sorted(category_names.dropna().unique())]
    _execute_many(cur, sql, names)
    cur.execute("SELECT category_key, category_name FROM dw.dim_category")
    return {row[1]: row[0] for row in cur.fetchall()}


def _load_dim_year(cur, award_df: pd.DataFrame) -> dict[int, int]:
    """UPSERT dim_year (year_key = natural key); return {year: year_key} lookup.

    ceremony_title: one title per year (invariant enforced by the transform gate).
    """
    year_map = (award_df.dropna(subset=["year", "ceremony_title"])
                        .drop_duplicates("year")[["year", "ceremony_title"]])
    sql = """
        INSERT INTO dw.dim_year (year_key, decade, ceremony_title)
        VALUES %s
        ON CONFLICT (year_key) DO UPDATE SET
            decade         = EXCLUDED.decade,
            ceremony_title = EXCLUDED.ceremony_title
    """
    rows = [(_as_py(r.year), _as_py(r.year) // 10 * 10, _as_py(r.ceremony_title))
            for r in year_map.itertuples(index=False)]
    _execute_many(cur, sql, rows)
    cur.execute("SELECT year_key FROM dw.dim_year")
    return {row[0]: row[0] for row in cur.fetchall()}   # year is its own key


# ---------------------------------------------------------------------------
# Fact loaders  (Truncate-and-Load strategy)
# ---------------------------------------------------------------------------

def _load_fact_track_credit(cur, tc_df: pd.DataFrame,
                             track_map: dict[str, int],
                             artist_map: dict[str, int],
                             genre_map: dict[str, int]) -> int:
    """Truncate-and-Load fact_track_credit inside the caller's transaction."""
    cur.execute("TRUNCATE dw.fact_track_credit RESTART IDENTITY")
    rows = []
    skipped = 0
    for r in tc_df.itertuples(index=False):
        tk = track_map.get(r.track_id)
        ak = artist_map.get(r.artist_match_key)
        gk = genre_map.get(r.genre_name)
        if tk is None or ak is None or gk is None:
            skipped += 1
            log.warning("fact_track_credit: missing key for track=%s artist=%s genre=%s — row skipped",
                        r.track_id, r.artist_match_key, r.genre_name)
            continue
        rows.append((tk, ak, gk, int(r.popularity),
                     float(r.danceability), float(r.energy), float(r.valence), float(r.acousticness)))
    if skipped:
        log.warning("fact_track_credit: %s rows skipped due to missing dimension keys", skipped)
    sql = """
        INSERT INTO dw.fact_track_credit
            (track_key, artist_key, genre_key, popularity,
             danceability, energy, valence, acousticness)
        VALUES %s
        ON CONFLICT (track_key, artist_key, genre_key) DO NOTHING
    """
    _execute_many(cur, sql, rows)
    return len(rows)


def _load_fact_grammy_award(cur, award_df: pd.DataFrame,
                             artist_map: dict[str, int],
                             category_map: dict[str, int],
                             year_map: dict[int, int]) -> int:
    """Truncate-and-Load fact_grammy_award inside the caller's transaction."""
    cur.execute("TRUNCATE dw.fact_grammy_award RESTART IDENTITY")
    rows = []
    skipped = 0
    for r in award_df.itertuples(index=False):
        ak  = artist_map.get(r.artist_match_key)
        ck  = category_map.get(r.category_name)
        yk  = year_map.get(int(r.year))
        if ak is None or ck is None or yk is None:
            skipped += 1
            log.warning("fact_grammy_award: missing key for award_id=%s artist=%s category=%s year=%s — row skipped",
                        r.award_id, r.artist_match_key, r.category_name, r.year)
            continue
        nominee = None if (r.nominee is None or (isinstance(r.nominee, float) and pd.isna(r.nominee))) else str(r.nominee)
        rows.append((_as_py(r.award_id), ak, ck, _as_py(r.year), nominee))
    if skipped:
        log.warning("fact_grammy_award: %s rows skipped due to missing dimension keys", skipped)
    sql = """
        INSERT INTO dw.fact_grammy_award
            (award_id, artist_key, category_key, year_key, nominee)
        VALUES %s
        ON CONFLICT (award_id, artist_key) DO NOTHING
    """
    _execute_many(cur, sql, rows)
    return len(rows)


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def load_to_dw(prepared_meta: dict) -> dict:
    """Read the four prepared Parquet files and load them into music_dw.

    Args:
        prepared_meta: the dict returned by validation.validate_prepared(), which includes
                       batch_id, paths (dict of dataset -> parquet path), validation_action.

    Returns:
        A metadata dict with row counts and load timing, mirroring the pattern of
        extract_spotify / extract_grammys.
    """
    batch_id   = prepared_meta["batch_id"]
    paths      = prepared_meta["paths"]

    dim_artist_df  = pd.read_parquet(paths["prepared_dim_artist"])
    dim_track_df   = pd.read_parquet(paths["prepared_dim_track"])
    track_credit   = pd.read_parquet(paths["prepared_track_credit"])
    award_credit   = pd.read_parquet(paths["prepared_award_credit"])

    started_at = datetime.now(timezone.utc)
    counts: dict[str, int] = {}

    conn = _conn(config.DW_DB)
    try:
        with conn:                    # single transaction for the whole load
            with conn.cursor() as cur:
                # ---- dimensions (UPSERT, order matters for FK safety) ----
                counts["dim_artist"]   = _load_dim_artist(cur, dim_artist_df)
                genre_map              = _load_dim_genre(cur, track_credit["genre_name"])
                track_map              = _load_dim_track(cur, dim_track_df)
                category_map           = _load_dim_category(cur, award_credit["category_name"])
                year_map               = _load_dim_year(cur, award_credit)
                counts["dim_genre"]    = len(genre_map)
                counts["dim_track"]    = len(track_map)
                counts["dim_category"] = len(category_map)
                counts["dim_year"]     = len(year_map)

                # artist_key lookup: match business key -> surrogate key
                cur.execute("SELECT artist_key, artist_match_key FROM dw.dim_artist")
                artist_map = {row[1]: row[0] for row in cur.fetchall()}

                # ---- facts (Truncate-and-Load, within the same transaction) ----
                counts["fact_track_credit"]  = _load_fact_track_credit(
                    cur, track_credit, track_map, artist_map, genre_map)
                counts["fact_grammy_award"] = _load_fact_grammy_award(
                    cur, award_credit, artist_map, category_map, year_map)

        # conn.__exit__ with no exception → COMMIT
    except Exception:
        # conn.__exit__ with an exception → ROLLBACK (psycopg2 transaction context manager)
        raise
    finally:
        conn.close()

    finished_at = datetime.now(timezone.utc)
    elapsed_s   = round((finished_at - started_at).total_seconds(), 2)

    summary = {
        "batch_id"     : batch_id,
        "loaded_at_utc": finished_at.isoformat(timespec="seconds"),
        "elapsed_s"    : elapsed_s,
        "target_db"    : config.DW_DB,
        "rows_loaded"  : counts,
        "validation_action_upstream": prepared_meta.get("validation_action"),
    }
    _write_evidence(batch_id, summary)
    log.info("[%s] load_to_dw completed in %.1fs: %s", batch_id, elapsed_s, counts)
    return summary
