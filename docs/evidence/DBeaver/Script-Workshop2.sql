SELECT 'dim_artist' AS tabla, count(*) FROM dw.dim_artist
UNION ALL
SELECT 'dim_track', count(*) FROM dw.dim_track
UNION ALL
SELECT 'fact_track_credit', count(*) FROM dw.fact_track_credit
UNION ALL
SELECT 'fact_grammy_award', count(*) FROM dw.fact_grammy_award;


-- Muestra el comparativo de popularidad entre grupos
SELECT 
    * 
FROM dw.vw_kpi_1a_popularity_by_grammy;

-- Muestra el porcentaje (9.36%) de pistas que tienen artistas galardonados
SELECT 
    * 
FROM dw.vw_kpi_1b_catalog_share;

-- Top 10 géneros con mayor cantidad de artistas ganadores de un Grammy
SELECT 
    * 
FROM dw.vw_kpi_2a_grammy_artists_by_genre 
ORDER BY pct_grammy_artists  DESC 
LIMIT 10;

-- Muestra las diferencias en features de audio (danceability, energy, etc.)
SELECT 
    * 
FROM dw.vw_kpi_2b_audio_profile
ORDER BY genre_name ASC;

-- Top 10 histórico de artistas con más premios Grammy y su presencia en Spotify
SELECT 
    * 
FROM dw.vw_kpi_3_artist_ranking 
ORDER BY awards DESC 
LIMIT 10;

