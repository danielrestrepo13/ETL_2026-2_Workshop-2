-- KPI views over the dimensional model (every output queries the DW, never the CSVs).
-- Rule: only artist_type = 'REAL' artists enter artist-level KPIs (placeholders and unknown are excluded).

-- One row per (artist, track): a track listed in several genres counts once; its popularity is the mean of its genre rows.
CREATE OR REPLACE VIEW dw.vw_artist_track AS
SELECT f.artist_key, f.track_key, AVG(f.popularity)::numeric(6,2) AS track_popularity
FROM dw.fact_track_credit f
JOIN dw.dim_artist a ON a.artist_key = f.artist_key AND a.artist_type = 'REAL'
GROUP BY f.artist_key, f.track_key;

-- KPI-1a (R1): average popularity, Grammy vs non-Grammy artists.
-- Headline metric = avg_popularity_excl_zero (popularity 0 stays in the facts; approved decision). avg_popularity and pct_zero_popularity give the context.
CREATE OR REPLACE VIEW dw.vw_kpi_1a_popularity_by_grammy AS
SELECT CASE WHEN a.has_grammy_awards THEN 'Grammy artist' ELSE 'Non-Grammy artist' END AS artist_group,
       COUNT(DISTINCT t.artist_key)                                      AS artists,
       COUNT(*)                                                          AS artist_track_pairs,
       ROUND(AVG(t.track_popularity), 2)                                 AS avg_popularity,
       ROUND(AVG(t.track_popularity) FILTER (WHERE t.track_popularity > 0), 2) AS avg_popularity_excl_zero,
       ROUND(100.0 * COUNT(*) FILTER (WHERE t.track_popularity = 0) / COUNT(*), 2) AS pct_zero_popularity
FROM dw.vw_artist_track t
JOIN dw.dim_artist a USING (artist_key)
GROUP BY a.has_grammy_awards;

-- KPI-1b (R1): share of catalog tracks with at least one Grammy artist.
CREATE OR REPLACE VIEW dw.vw_kpi_1b_catalog_share AS
SELECT COUNT(DISTINCT f.track_key) FILTER (WHERE a.artist_type = 'REAL' AND a.has_grammy_awards) AS tracks_with_grammy_artist,
       COUNT(DISTINCT f.track_key)                                                                AS tracks_total,
       ROUND(100.0 * COUNT(DISTINCT f.track_key) FILTER (WHERE a.artist_type = 'REAL' AND a.has_grammy_awards)
             / NULLIF(COUNT(DISTINCT f.track_key), 0), 2)                                         AS pct_tracks_grammy
FROM dw.fact_track_credit f
JOIN dw.dim_artist a USING (artist_key);

-- KPI-2a (R2): Grammy artists per genre.
CREATE OR REPLACE VIEW dw.vw_kpi_2a_grammy_artists_by_genre AS
SELECT g.genre_name,
       COUNT(DISTINCT f.artist_key) FILTER (WHERE a.has_grammy_awards) AS grammy_artists,
       COUNT(DISTINCT f.artist_key)                                    AS artists_total,
       ROUND(100.0 * COUNT(DISTINCT f.artist_key) FILTER (WHERE a.has_grammy_awards)
             / NULLIF(COUNT(DISTINCT f.artist_key), 0), 2)             AS pct_grammy_artists
FROM dw.fact_track_credit f
JOIN dw.dim_genre  g USING (genre_key)
JOIN dw.dim_artist a ON a.artist_key = f.artist_key AND a.artist_type = 'REAL'
GROUP BY g.genre_name;

-- KPI-2b (R2): audio profile by genre and group. Each (track, genre, group) counts once.
CREATE OR REPLACE VIEW dw.vw_kpi_2b_audio_profile AS
WITH tg AS (
    SELECT DISTINCT f.track_key, f.genre_key, a.has_grammy_awards AS is_grammy,
           f.danceability, f.energy, f.valence, f.acousticness
    FROM dw.fact_track_credit f
    JOIN dw.dim_artist a ON a.artist_key = f.artist_key AND a.artist_type = 'REAL'
)
SELECT g.genre_name,
       CASE WHEN tg.is_grammy THEN 'Grammy artist' ELSE 'Non-Grammy artist' END AS artist_group,
       COUNT(*)                          AS tracks,
       ROUND(AVG(tg.danceability), 3)    AS avg_danceability,
       ROUND(AVG(tg.energy), 3)          AS avg_energy,
       ROUND(AVG(tg.valence), 3)         AS avg_valence,
       ROUND(AVG(tg.acousticness), 3)    AS avg_acousticness
FROM tg JOIN dw.dim_genre g USING (genre_key)
GROUP BY g.genre_name, tg.is_grammy;

-- KPI-3a / KPI-3b (R3): awards vs Spotify presence per artist. Top 10: ORDER BY awards DESC LIMIT 10.
CREATE OR REPLACE VIEW dw.vw_kpi_3_artist_ranking AS
WITH awards AS (
    SELECT artist_key, COUNT(DISTINCT award_id) AS awards, COUNT(DISTINCT category_key) AS categories,
           MIN(year_key) AS first_award_year, MAX(year_key) AS last_award_year
    FROM dw.fact_grammy_award GROUP BY artist_key),
tracks AS (
    SELECT artist_key, COUNT(*) AS tracks,
           ROUND(AVG(track_popularity) FILTER (WHERE track_popularity > 0), 2) AS avg_popularity
    FROM dw.vw_artist_track GROUP BY artist_key)
SELECT a.artist_name, aw.awards, aw.categories, aw.first_award_year, aw.last_award_year,
       COALESCE(t.tracks, 0) AS tracks, t.avg_popularity
FROM awards aw
JOIN dw.dim_artist a ON a.artist_key = aw.artist_key AND a.artist_type = 'REAL'
LEFT JOIN tracks t ON t.artist_key = aw.artist_key;