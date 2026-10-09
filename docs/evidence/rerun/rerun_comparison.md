# Safe Rerun Evidence
El pipeline se ejecutó dos veces consecutivas. La estrategia UPSERT (dimensiones) y Truncate-and-Load (hechos) garantizó que las filas no se duplicaran.
- **Antes del Rerun:** `dim_artist` (30,763), `fact_track_credit` (157,531).
- **Después del Rerun:** `dim_artist` (30,763), `fact_track_credit` (157,531).
*Evidencia en DBeaver adjunta.*