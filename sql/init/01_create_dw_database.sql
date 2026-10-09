-- Runs once, when the analytics-postgres volume is first created.
-- POSTGRES_DB already created grammy_source (operational source).
CREATE DATABASE music_dw;   -- analytical Data Warehouse (star schema)
