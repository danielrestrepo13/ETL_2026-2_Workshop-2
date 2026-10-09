"""Transformation and integration (6.8). One engineering phase: clean, standardize, split, integrate, derive.

Input : staged raw Spotify and Grammy Parquet files (already through their raw validation gate).
Output: four prepared Parquet files (dim_artist, dim_track, track_credit, award_credit) + a reconciliation report.
Every rule below is documented in docs/transformation_decisions.md and is defensible without reference to the
validation result.
"""
import json
import logging
import re
import unicodedata

import pandas as pd

from src import config

log = logging.getLogger(__name__)

UNKNOWN_KEY, PLACEHOLDER_KEY = "__unknown__", "__placeholder__"
# Credits that do not identify an act (approved decision: map to the PLACEHOLDER member).
GENERIC_CREDITS = {"variousartists", "originalcast", "originalbroadwaycast"}
SPLIT_RE = re.compile(r"\s+(?:feat\.?|featuring|ft\.?|with|and|&|\+)\s+|\s*[,;/]\s*", re.IGNORECASE)
AUDIO = ["danceability", "energy", "valence", "acousticness"]


class ReconciliationError(Exception):
    """An integration invariant failed (rows or keys lost/duplicated): deterministic, not retryable."""


def norm_key(value) -> str | None:
    """Matching key: accents removed, case folded, everything but letters/digits dropped."""
    if value is None or pd.isna(value):          # covers None, NaN and pandas NA
        return None
    s = unicodedata.normalize("NFKD", str(value))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"[\W_]+", "", s.casefold()) or None


def strip_parens(name: str) -> str:
    m = re.fullmatch(r"\((.*)\)", name.strip())
    return (m.group(1) if m else name).strip()


def artist_key(display: str | None) -> str:
    """Integration key of one artist credit, with the special members for missing / generic credits."""
    k = norm_key(display)
    if k is None:
        return UNKNOWN_KEY
    return PLACEHOLDER_KEY if k in GENERIC_CREDITS else k


def split_grammy_artist(raw, spotify_keys: set):
    """Conservative split of a Grammy artist string into credits. Returns (list[(display, key)], mode).

    * whole_match   : the whole string is already an artist in Spotify (e.g. 'Earth, Wind & Fire') -> not split
    * split         : at least one part is a Spotify artist -> evidence that it is a collaboration -> split
    * kept_unsplit  : no evidence either way -> kept whole (avoids breaking band names such as 'Simon & Garfunkel')
    * unknown       : missing artist
    """
    if raw is None or pd.isna(raw) or not str(raw).strip():
        return [(None, UNKNOWN_KEY)], "unknown"
    name = strip_parens(str(raw))
    whole = artist_key(name)
    if whole == PLACEHOLDER_KEY:
        return [(name, PLACEHOLDER_KEY)], "generic"
    if whole in spotify_keys:
        return [(name, whole)], "whole_match"
    parts = [p.strip() for p in SPLIT_RE.split(name) if p and p.strip()]
    if len(parts) > 1:
        credits = [(p, artist_key(p)) for p in parts]
        if any(k in spotify_keys for _, k in credits):
            return credits, "split"
    return [(name, whole)], "kept_unsplit"


def _display_names(pairs: pd.DataFrame) -> pd.Series:
    """One display name per key: most frequent written form, ties broken alphabetically."""
    counts = pairs.groupby(["artist_match_key", "display"]).size().reset_index(name="n")
    counts = counts.sort_values(["artist_match_key", "n", "display"], ascending=[True, False, True])
    return counts.drop_duplicates("artist_match_key").set_index("artist_match_key")["display"]


def transform_and_integrate(spotify_meta: dict, grammy_meta: dict) -> dict:
    batch_id = spotify_meta["batch_id"]
    sp_raw = pd.read_parquet(spotify_meta["path"])
    gr_raw = pd.read_parquet(grammy_meta["path"])
    rec = {"batch_id": batch_id, "spotify_raw_rows": int(len(sp_raw)), "grammy_raw_rows": int(len(gr_raw))}

    # ------------------------------------------------------------------ Spotify
    sp = sp_raw.drop(columns=["Unnamed: 0"], errors="ignore")                       # T1 index artifact
    for c in ["track_id", "track_name", "album_name", "artists", "track_genre"]:    # T2 trim
        sp[c] = sp[c].astype("string").str.strip()
    n0 = len(sp)
    sp = sp.drop_duplicates().reset_index(drop=True)                                # T3 exact duplicates
    rec["spotify_exact_duplicates_removed"] = int(n0 - len(sp))
    bad_dur = sp["duration_ms"] <= 0                                                # T4 impossible duration -> NULL
    rec["spotify_duration_set_null"] = int(bad_dur.sum())
    sp.loc[bad_dur, "duration_ms"] = pd.NA
    sp["duration_ms"] = sp["duration_ms"].astype("Int64")

    sp_unknown_expected = int(sp["artists"].isna().sum() + (sp["artists"].fillna("x").str.strip() == "").sum())
    sp["artist_display"] = sp["artists"].fillna("").str.split(";")                  # T5 split artists
    sp = sp.explode("artist_display")
    sp["artist_display"] = sp["artist_display"].str.strip().replace("", pd.NA)
    sp["artist_match_key"] = sp["artist_display"].map(artist_key)
    before = len(sp)
    credits = sp.drop_duplicates(["track_id", "artist_match_key", "track_genre"]).copy()     # T6 grain
    rec["spotify_credit_duplicates_removed"] = int(before - len(credits))
    track_credit = credits.rename(columns={"track_genre": "genre_name"})[
        ["track_id", "artist_match_key", "genre_name", "popularity"] + AUDIO].reset_index(drop=True)
    track_credit["popularity"] = track_credit["popularity"].astype(int)

    track_cols = ["track_name", "album_name", "explicit", "duration_ms"]
    inconsistent = sp.groupby("track_id")[track_cols].nunique(dropna=False).gt(1).any(axis=1)
    rec["spotify_tracks_with_inconsistent_attributes"] = int(inconsistent.sum())
    dim_track = (sp.sort_values(["track_id", "track_genre"]).drop_duplicates("track_id")[["track_id"] + track_cols]
                 .reset_index(drop=True))                                              # first row per track, deterministic
    sp_keys = set(track_credit.loc[~track_credit["artist_match_key"].isin([UNKNOWN_KEY, PLACEHOLDER_KEY]),
                                   "artist_match_key"])

    # ------------------------------------------------------------------ Grammy
    gr = gr_raw.copy()
    for c in ["category", "nominee", "artist", "title"]:
        gr[c] = gr[c].astype("string").str.strip()
    rows, modes = [], {"whole_match": 0, "split": 0, "kept_unsplit": 0, "unknown": 0, "generic": 0}
    for rec_row in gr.itertuples(index=False):
        parts, mode = split_grammy_artist(rec_row.artist, sp_keys)
        modes[mode] += 1
        for display, key in parts:
            rows.append((int(rec_row.source_row_id), key, display, rec_row.category, int(rec_row.year),
                         rec_row.title, rec_row.nominee))
    award = pd.DataFrame(rows, columns=["award_id", "artist_match_key", "display", "category_name", "year",
                                        "ceremony_title", "nominee"])
    before = len(award)
    award = award.drop_duplicates(["award_id", "artist_match_key"]).reset_index(drop=True)
    rec["grammy_credit_duplicates_removed"] = int(before - len(award))
    rec["grammy_artist_split_modes"] = modes
    real = ~award["artist_match_key"].isin([UNKNOWN_KEY, PLACEHOLDER_KEY])
    award["matched_in_spotify"] = award["artist_match_key"].isin(sp_keys).astype(float).where(real)

    # ------------------------------------------------------------------ dim_artist
    pairs = pd.concat([
        sp.loc[~sp["artist_match_key"].isin([UNKNOWN_KEY, PLACEHOLDER_KEY]), ["artist_match_key", "artist_display"]]
          .rename(columns={"artist_display": "display"}),
        award.loc[real, ["artist_match_key", "display"]]])
    names = _display_names(pairs.dropna())
    gr_keys = set(award.loc[real, "artist_match_key"])
    dim_artist = pd.DataFrame({"artist_match_key": sorted(sp_keys | gr_keys)})
    dim_artist["artist_name"] = dim_artist["artist_match_key"].map(names)
    dim_artist["artist_type"] = "REAL"
    dim_artist["has_spotify_tracks"] = dim_artist["artist_match_key"].isin(sp_keys).astype(int)
    dim_artist["has_grammy_awards"] = dim_artist["artist_match_key"].isin(gr_keys).astype(int)
    special = pd.DataFrame([
        {"artist_match_key": UNKNOWN_KEY, "artist_name": "Unknown (no artist in source)", "artist_type": "UNKNOWN",
         "has_spotify_tracks": int((track_credit["artist_match_key"] == UNKNOWN_KEY).any()),
         "has_grammy_awards": int((award["artist_match_key"] == UNKNOWN_KEY).any())},
        {"artist_match_key": PLACEHOLDER_KEY, "artist_name": "Generic credit (Various Artists, Original Cast)",
         "artist_type": "PLACEHOLDER",
         "has_spotify_tracks": int((track_credit["artist_match_key"] == PLACEHOLDER_KEY).any()),
         "has_grammy_awards": int((award["artist_match_key"] == PLACEHOLDER_KEY).any())}])
    dim_artist = pd.concat([dim_artist, special], ignore_index=True)
    # special members never count as Grammy-linked artists in KPIs
    dim_artist.loc[dim_artist["artist_type"] != "REAL", "has_grammy_awards"] = 0
    # integration indicator used by the prepared gate (DQ19); not stored in the DW
    dim_artist["grammy_spotify_overlap"] = dim_artist["has_spotify_tracks"] * dim_artist["has_grammy_awards"]

    award = award.drop(columns=["display"])

    # ------------------------------------------------------------------ reconciliation invariants
    checks = {
        "tracks_preserved": sp_raw["track_id"].nunique() == dim_track["track_id"].nunique() == track_credit["track_id"].nunique(),
        "genres_preserved": set(sp_raw["track_genre"].str.strip()) == set(track_credit["genre_name"]),
        "awards_preserved": set(gr_raw["source_row_id"]) == set(award["award_id"]),
        "artist_key_unique": bool(dim_artist["artist_match_key"].is_unique),
        "every_credit_key_in_dim": set(track_credit["artist_match_key"]) | set(award["artist_match_key"]) <= set(dim_artist["artist_match_key"]),
        "one_ceremony_title_per_year": bool(award.groupby("year")["ceremony_title"].nunique().max() == 1),
        # missing artists must become the UNKNOWN member, never a fake artist
        "grammy_unknown_equals_source_nulls": int((award["artist_match_key"] == UNKNOWN_KEY).sum())
                                             == int(gr_raw["artist"].isna().sum() + (gr_raw["artist"].astype("string").str.strip() == "").sum()),
        "spotify_unknown_equals_source_nulls": int(((credits["artist_match_key"] == UNKNOWN_KEY)).sum())
                                              == int(sp_unknown_expected),
    }
    rec["invariants"] = checks

    real_awards = award[~award["artist_match_key"].isin([UNKNOWN_KEY, PLACEHOLDER_KEY])]
    unmatched = real_awards[real_awards["matched_in_spotify"] == 0]
    rec["integration"] = {
        "spotify_real_artist_keys": len(sp_keys), "grammy_real_artist_keys": len(gr_keys),
        "matched_keys": len(sp_keys & gr_keys),
        "pct_grammy_keys_matched": round(100 * len(sp_keys & gr_keys) / max(len(gr_keys), 1), 2),
        "award_credits_total": int(len(award)),
        "award_credits_unknown": int((award["artist_match_key"] == UNKNOWN_KEY).sum()),
        "award_credits_placeholder": int((award["artist_match_key"] == PLACEHOLDER_KEY).sum()),
        "award_credits_real": int(len(real_awards)),
        "award_credits_real_matched": int((real_awards["matched_in_spotify"] == 1).sum()),
        "pct_real_award_credits_matched": round(100 * (real_awards["matched_in_spotify"] == 1).mean(), 2),
        "spotify_tracks_with_grammy_artist": int(track_credit[track_credit["artist_match_key"].isin(sp_keys & gr_keys)]["track_id"].nunique()),
        "spotify_tracks_total": int(track_credit["track_id"].nunique()),
        "unmatched_top_artists": unmatched.merge(dim_artist[["artist_match_key", "artist_name"]], on="artist_match_key")
                                         .groupby("artist_name").size().sort_values(ascending=False).head(15).to_dict(),
        "duplicate_match_keys_in_dim": int(dim_artist["artist_match_key"].duplicated().sum()),
    }
    rec["rows_out"] = {"dim_artist": len(dim_artist), "dim_track": len(dim_track),
                       "track_credit": len(track_credit), "award_credit": len(award)}
    failed = [k for k, v in checks.items() if not v]
    if failed:
        raise ReconciliationError(f"Integration invariants failed: {failed}")

    # ------------------------------------------------------------------ outputs
    from src.extract import staging_dir
    out = staging_dir(batch_id)
    paths = {}
    for name, df in {"prepared_dim_artist": dim_artist, "prepared_dim_track": dim_track,
                     "prepared_track_credit": track_credit, "prepared_award_credit": award}.items():
        paths[name] = str(out / f"{name}.parquet")
        df.to_parquet(paths[name], index=False)
    ev = config.EVIDENCE_DIR / "transform" / batch_id
    ev.mkdir(parents=True, exist_ok=True)
    (ev / "reconciliation.json").write_text(json.dumps(rec, indent=2, default=str), encoding="utf-8")
    log.info("[%s] transform_and_integrate: %s", batch_id, rec["rows_out"])
    return {"batch_id": batch_id, "paths": paths, "rows": rec["rows_out"],
            "reconciliation": str(ev / "reconciliation.json")}