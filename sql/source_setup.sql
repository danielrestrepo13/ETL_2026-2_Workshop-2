-- Source preparation (NOT the ETL Load): operational Grammy source in database grammy_source.
-- Faithful copy of the provided CSV: no cleaning, NULLs preserved, original column names.
-- Decisions:
--   * source_row_id: technical identity key (the CSV has no natural key).
--   * published_at / updated_at: TIMESTAMPTZ (all values in the CSV parse with offset).
--   * Idempotent: CREATE ... IF NOT EXISTS; the loader truncates and reloads in one transaction.

CREATE TABLE IF NOT EXISTS public.grammy_awards (
    source_row_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    year          INTEGER,
    title         TEXT,
    published_at  TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ,
    category      TEXT,
    nominee       TEXT,
    artist        TEXT,
    workers       TEXT,
    img           TEXT,
    winner        BOOLEAN
);

-- One row per source load: evidence for row-count reconciliation.
CREATE TABLE IF NOT EXISTS public.source_load_audit (
    audit_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    csv_file     TEXT        NOT NULL,
    csv_sha256   TEXT        NOT NULL,
    csv_rows     INTEGER     NOT NULL,
    db_rows      INTEGER     NOT NULL,
    reconciled   BOOLEAN     NOT NULL
);
