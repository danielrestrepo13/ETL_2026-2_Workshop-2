# Transformation and Integration Decision Record (6.8 / 6.9)

Code: `src/transform.py` (rules), `src/extract.py` (6.7), `src/validation.py` (gates). Tests: `tests/test_transform.py`.
Figures come from a full local run (`python -m src.run_local_pipeline`); the reconciliation report of every batch is saved in
`docs/evidence/transform/<batch_id>/reconciliation.json`. Risk IDs (`RKxx`) refer to `docs/evidence/profiling/risk_register.md`.

Every rule is defensible on its own (profiling evidence or requirement); none exists to make a validation pass.

## 1. Pipeline stages (6.7 - 6.9)

| Stage | Task | What it does | Output (handoff) |
|---|---|---|---|
| Extract | `extract_spotify` | Reads the Spotify CSV, no cleaning. | `spotify_raw.parquet` + metadata dict |
| Extract | `extract_grammys` | Reads `grammy_source.public.grammy_awards` **from PostgreSQL** (query and host recorded; `grammy_csv_read = false`). | `grammy_raw.parquet` + metadata dict |
| Raw gate | `validate_spotify_raw`, `validate_grammys_raw` | Suites `spotify_raw_suite` / `grammy_raw_suite` (DQ01-DQ13); Critical failure raises `DataQualityError`. | metadata + action |
| Transform | `transform_and_integrate` | Rules T1-T11 below; reconciliation invariants. | 4 prepared Parquet files + `reconciliation.json` |
| Prepared gate | `validate_prepared` | Three suites (DQ14-DQ20); all run, then any Critical blocks `load_dw`. | metadata + action |

Tasks exchange **paths and compact metadata**, never DataFrames, so the orchestrator's metadata store is not used as a data bus.

## 2. Transformation decisions

| ID | Rule and rationale | Affected fields | Before -> after | Exception handling | Analytical impact |
|---|---|---|---|---|---|
| T1 | Drop the export index column (RK: equals the row index, not a business attribute). | Spotify `Unnamed: 0` | column removed | none | none |
| T2 | Trim leading/trailing spaces in text keys and labels. | `track_id`, names, `artists`, `track_genre`; Grammy `category`, `artist`, `nominee`, `title` | `" Pop"` -> `"Pop"` | none | avoids false distinct values |
| T3 | Remove exact duplicate rows (RK07: 450 rows, all identical). | all Spotify columns | 114,000 -> 113,550 rows | If duplicates differ in any value they are kept and resolved at the grain (T6). | counts and averages are not inflated |
| T4 | `duration_ms <= 0` is impossible for a song: set NULL instead of inventing a value. | Spotify `duration_ms` | 1 row: `0` -> NULL | `dim_track.duration_ms` is nullable | descriptive only; no KPI uses it |
| T5 | Split Spotify `artists` on `;` into one credit per artist (RK14: 26% of rows are multi-artist). | `artists` | `"A;B"` -> two rows | Empty or missing artist -> UNKNOWN member (T8) | makes collaborations matchable (R1-R3) |
| T6 | Enforce the fact grain: one row per (track, artist, genre). | credit rows | 0 additional duplicates removed after T5 | Keeps the first row deterministically | `UNIQUE` grain constraint can never fail |
| T7 | Grammy artist: strip outer parentheses (formatting only) and split collaborations **only with evidence** (see section 3). | Grammy `artist` | `"(Miles Davis)"` -> `Miles Davis`; `"Bruno Mars Featuring Cardi B"` -> two credits | No evidence -> kept whole, so band names such as "Simon & Garfunkel" are not broken | recovers collaborations (RK23) without inventing artists |
| T8 | Missing artist -> member `__unknown__` (Grammy: 1,840 rows; Spotify: 1 credit). Generic credits (`Various Artists`, `Original Cast`, `Original Broadway Cast`) -> member `__placeholder__` (Grammy: 69 rows, 76 credits incl. splits; Spotify: 1 credit). Approved decision. | `artist`, `artists` | `NULL` -> `__unknown__` | Special members are excluded from artist KPIs (`artist_type <> 'REAL'`) | keeps every award in the counts; they never become "top artist" |
| T9 | Matching key = accents removed, case folded, non-alphanumerics dropped; display name = most frequent written form (ties alphabetical). | `artist_match_key`, `artist_name` | `"Beyoncé"`, `"beyonce"` -> `beyonce` | A name that normalizes to empty -> UNKNOWN | one row per artist across both sources |
| T10 | Derive integration indicators: `has_spotify_tracks`, `has_grammy_awards`, `grammy_spotify_overlap`, `matched_in_spotify`. | `dim_artist`, `award_credit` | new columns | `matched_in_spotify` is NULL for special members | defines the Grammy group of R1-R3 |
| T11 | Enforce the award grain: one row per (award entry, artist). | Grammy credits | 2 duplicate credits removed | first kept | award counts are not inflated |
| T12 | *No transformation*: `popularity = 0` stays in the facts (approved); KPI views filter it. | `popularity` | unchanged | none | KPI-1a / KPI-3b exclude zeros and report the zero share |
| T13 | *Not carried*: `winner` is constant `True` (RK11); the DW measure is `award_count`. | `winner` | dropped | DQ12 stops the pipeline if it ever changes | no win rate |

## 3. Integration Contract

| Item | Team decision and evidence |
|---|---|
| **Integration key(s)** | Spotify `artists` (split on `;`) and Grammy `artist` -> `artist_match_key` (T9). No shared id exists, so integration is **by normalized artist name**. |
| **Cardinality** | Verified on the prepared data. Spotify: tracks per artist, many (median 5, max 332 for artists in both sources); credits per track up to 38. Grammy: awards per artist, many (median 1, max 18). Between the two sources the relation through the key is **many-to-many**, which is why the model has two facts that meet only in `dim_artist`. `dim_artist` has 0 duplicate keys (one-to-one key to dimension row). |
| **Preprocessing** | T2, T5, T7, T9 before matching. Grammy splitting is conditional: whole string already in Spotify -> not split (1,324 rows); some part in Spotify -> split (344 rows); no evidence -> kept whole (1,233 rows). |
| **Unmatched records** | Measured, not dropped. Of 1,737 real Grammy artist keys, 717 (41.28%) exist in Spotify; 1,020 are Grammy-only. Of 3,462 real award credits, 1,894 (54.71%) match. Largest unmatched acts (genuinely absent from the Spotify sample): U2 (18), Dixie Chicks (12), Jimmy Sturr (12), CeCe Winans (11), Pat Metheny Group (10). 1,840 Grammy rows have no artist and are kept as UNKNOWN. Rule DQ20 monitors the match share (Warning below 33%). |
| **Duplicate matches** | A key is unique in `dim_artist` (DQ14, 0 duplicates). Different written forms of one artist collapse into one key by design; different people with the same name would also collapse (homonyms, see limitations). |
| **Assumptions and limitations** | (1) Same normalized name = same artist: homonyms may be joined and spelling variants ("The Beatles" / "Beatles") may be missed. (2) A Grammy string is split only when a part is a known Spotify artist, so collaborations between acts absent from Spotify stay unsplit; this affects award counts of artists outside the overlap, not the overlap itself. (3) Spotify has no date, so no temporal claim ("after winning") can be made. (4) The Spotify file is a sample: 717 overlapping artists describe the overlap, not all Grammy history. |

## 4. Reconciliation (evidence of correct integration)

Invariants checked on every batch; a failure raises `ReconciliationError` (deterministic, no retry):

| Invariant | Result |
|---|---|
| Distinct `track_id`: raw = `dim_track` = credits (89,741) | pass |
| Genres preserved (same set in raw and prepared) | pass |
| Every Grammy `source_row_id` appears in the award credits (4,810 entries -> 5,378 credits) | pass |
| `artist_match_key` unique; every credit key exists in `dim_artist` | pass |
| One ceremony title per year | pass |
| UNKNOWN credits = missing artists in the source (Grammy 1,840; Spotify 1) | pass |

Row counts: Spotify 114,000 raw -> 113,550 after T3 -> 157,531 track credits; Grammy 4,810 raw -> 5,378 award credits; `dim_artist` 30,763 (29,741 Spotify keys + 1,737 Grammy keys - 717 shared + 2 special members); `dim_track` 89,741.

The last two invariants were added after the first run: pandas missing values (`pd.NA`) were being converted to the text "<NA>" and created a false artist called "na" holding 1,840 awards. The data was still valid in form, so no quality rule could see it; only comparing the UNKNOWN count against the source nulls exposes it. `tests/test_transform.py` keeps a regression test for it.

## 5. Prepared validation (6.9)

`validate_prepared` ran the three prepared suites on the real batch: `prepared_dim_artist`, `prepared_track_credit`, `prepared_award_credit` -> **CONTINUE** (0 failed rules; DQ19: 717 artists in both sources >= 30; DQ20: 54.71% matched >= 33%). The gate logic is also tested with synthetic failures for each of DQ14-DQ20 (`tests/test_prepared_suites.py`). The Critical outcome of this gate is what will allow `load_dw` to run (dependency defined in the DAG, 6.11).