# Analytical Requirements and Solution Scope

**Workshop-2 — Reliable Batch Data Pipeline (ETL G01)**
Traceability IDs defined here (`R1-R3`, `KPI-x`, `V-x`) are reused in profiling, quality rules, GX expectations, transformations, the DW schema and the dashboard.

## 1. Scope statement

The pipeline integrates the Spotify track catalog (CSV, 114,000 rows) with the Grammy Awards history 1958-2019 (relational source DB, 4,810 rows) into a PostgreSQL star schema. Sources are related **by artist** (normalized name, many-to-many), not by time: Spotify has no release date, so temporal analysis is out of scope. Tracks whose artists have no Grammy record are kept (they form the comparison group). Grammy rows without an artist (e.g. technical or classical categories) cannot be integrated and are measured as unmatched, not discarded silently.

## 2. Analytical requirements

| ID | Analytical requirement | Required data (attribute -> source) | Source(s) | Expected KPI(s) | Required level of detail |
|---|---|---|---|---|---|
| **R1** | Are artists with Grammy recognition more popular on Spotify than artists without it? Supports decisions on whether awards are a useful signal of audience reach. | `artists`, `popularity`, `track_id` -> Spotify; `artist` -> Grammy | Spotify CSV + Grammy DB | **KPI-1a** avg popularity, Grammy vs non-Grammy artists; **KPI-1b** % of catalog tracks by Grammy artists | Artist |
| **R2** | Which genres concentrate Grammy artists, and how does their audio profile differ from the rest? Supports genre-level positioning. | `track_genre`, `danceability`, `energy`, `valence`, `acousticness` -> Spotify; `artist` -> Grammy | Spotify CSV + Grammy DB | **KPI-2a** # Grammy artists per genre; **KPI-2b** avg audio features, Grammy vs non-Grammy, per genre | Genre (and Grammy flag) |
| **R3** | Do artists with more Grammy awards have a larger Spotify presence and higher popularity? Supports artist ranking and benchmarking. | `artist`, `category`, `year` -> Grammy; `track_id`, `popularity` -> Spotify | Spotify CSV + Grammy DB | **KPI-3a** # awards, # distinct categories, first/last award year per artist; **KPI-3b** # tracks and avg popularity per artist; Top 10 | Artist |

## 3. Why both sources are required

- Spotify alone has popularity and audio features but no recognition signal.
- Grammy alone has recognition but no audience or audio information.
- Every KPI compares or relates a Grammy attribute with a Spotify attribute, so none can be answered from a single source.

## 4. Planned analytical outputs

| ID | Requirement | Output |
|---|---|---|
| V-1 | R1 | Bar chart: avg popularity, Grammy vs non-Grammy (KPI-1a, KPI-1b card) |
| V-2 | R2 | Bar/heatmap by genre: Grammy artists and audio features (KPI-2a, KPI-2b) |
| V-3 | R3 | Scatter: # awards vs avg popularity + Top 10 table (KPI-3a, KPI-3b) |

All outputs query the dimensional DW, never the CSVs.

## 5. Working assumptions (to be confirmed by profiling)

- A1: Integration key = normalized artist name (lowercase, no punctuation); Spotify `artists` is split on `;`.
- A2: "Grammy artist" = artist present in the Grammy table. In the provided file `winner` is `True` in all 4,810 rows, so the data holds awarded entries only: there are no nominee-only rows and no win rate can be computed. Measures are counts of awards, not nominations. Profiling must record this.
- A3: Spotify `track_id` repeats across genres; the grain and de-duplication rule are decided in the dimensional design (6.4) and transformation (6.8).
- A4: Name matching can produce false positives (homonyms) and misses (spelling variants); limits are documented in the Integration Contract.

## 6. Traceability stub (completed as the project advances)

| Requirement | Quality risk | DQ rule | GX expectation | Transformation | DW element | KPI / Viz |
|---|---|---|---|---|---|---|
| R1 | RK07, RK08, RK14, RK22 (6.3) | DQ03, DQ05, DQ06, DQ07, DQ15, DQ18, DQ19 | `ExpectColumnValuesToBeBetween(popularity)`, `ExpectCompoundColumnsToBeUnique(track_id, track_genre)`, `ExpectColumnSumToBeBetween(grammy_spotify_overlap)` | T3, T5, T6, T9, T12 | `fact_track_credit`, `dim_artist`, `vw_kpi_1a/1b` | KPI-1a/1b, V-1 |
| R2 | RK13, RK16, RK14 | DQ04, DQ18, DQ19 | `ExpectColumnValuesToBeBetween(danceability, energy, valence, acousticness)` | T5, T9, T10 | `fact_track_credit`, `dim_genre`, `vw_kpi_2a/2b` | KPI-2a/2b, V-2 |
| R3 | RK05, RK11, RK23, RK24, RK25 | DQ09-DQ13, DQ16, DQ17, DQ20 | `ExpectColumnValuesToBeInSet(winner)`, `ExpectColumnValuesToNotBeNull(artist)`, `ExpectColumnMeanToBeBetween(matched_in_spotify)` | T7, T8, T9, T10, T11 | `fact_grammy_award`, `dim_category`, `dim_year`, `vw_kpi_3` | KPI-3a/3b, V-3 |