# Workshop 2: Building a Reliable Batch Data Pipeline

**Asignatura:** ETL - Ingeniería de Datos e Inteligencia Artificial (2026-2)  
**Institución:** Universidad Autónoma de Occidente (UAO)  
**Stack tecnológico:** Python 3.12, PostgreSQL 16, Apache Airflow 3.1.8, Great Expectations 1.23.x, Docker & Docker Compose.

---

## 1. Descripción de la Arquitectura y Fuentes

El objetivo de este proyecto es construir un pipeline de datos batch confiable, resiliente e idempotente que integra dos fuentes heterogéneas para alimentar un Data Warehouse analítico con modelo en estrella en PostgreSQL (`music_dw`), respondiendo a preguntas de negocio sobre la relación entre el reconocimiento en los premios Grammy y la popularidad/características acústicas en Spotify.

```
       +-----------------------+              +------------------------------------+
       |  Spotify Tracks CSV   |              | Grammy Awards (PostgreSQL Source)  |
       |  (114,000 registros)  |              |       (4,810 registros)            |
       +-----------+-----------+              +-----------------+------------------+
                   |                                            |
                   v                                            v
         [ extract_spotify ]                          [ extract_grammys ]
                   |                                            |
                   v                                            v
         ( spotify_raw.parquet )                      ( grammy_raw.parquet )
                   |                                            |
                   v                                            v
       [ validate_raw_spotify ]                     [ validate_raw_grammys ]
       (Great Expectations Gate)                    (Great Expectations Gate)
                   \                                            /
                    \                                          /
                     v                                        v
                 +-----------------------------------------------+
                 |            transform_and_integrate           |
                 |      (Limpieza, Split N:M, Normalización,     |
                 |       Miembros Especiales, Reconciliación)   |
                 +-----------------------+-----------------------+
                                         |
                                         v
                         ( 4 Datasets Preparados Parquet )
                                         |
                                         v
                            [ validate_prepared ]
                          (Great Expectations Gate)
                                         |
                                         v
                                  [ load_dw ]
                     (Safe Rerun: UPSERT dims + Truncate-and-Load facts)
                                         |
                                         v
                 +-----------------------------------------------+
                 |       PostgreSQL Data Warehouse (music_dw)    |
                 |            Esquema Estrella ('dw')            |
                 |      Dimensiones + Hechos + Vistas KPI        |
                 +-----------------------------------------------+
```

### Fuentes de Datos
1. **Spotify Dataset (`data/raw/spotify_dataset.csv`):**
   - Catálogo de 114,000 pistas con atributos de audio (`danceability`, `energy`, `valence`, `acousticness`), `popularity`, `artists` (delimitados por `;`) y `track_genre`.
2. **Grammy Awards Source DB (`grammy_source.public.grammy_awards`):**
   - Base de datos relacional operacional en PostgreSQL (puerto 5433 / contenedor `analytics-postgres`) con 4,810 registros históricos (1958–2019). **Se extrae exclusivamente mediante consulta SQL relacional**, nunca de archivos planos intermedios.

### Principios de Ingeniería y Confiabilidad
- **Arquitectura desacoplada:** Las tareas intercambian únicamente metadatos compactos (`batch_id`, rutas Parquet, conteos), nunca DataFrames masivos por el metadata store de Airflow (XCom).
- **Puertas de calidad (Quality Gates):** Validación estricta con Great Expectations en la capa cruda (`raw`) y en la capa de integración (`prepared`).
- **Política de Severidad:**
  - `Critical`: Falla determinística de datos -> Bloqueo inmediato del pipeline (`STOP`). No reintentable.
  - `Warning`: Alerta registrada en bitácora/JSON -> Permite continuar (`CONTINUE WITH WARNING`).
- **Estrategia Safe Rerun (Idempotencia):**
  - Dimensiones: `UPSERT` (`INSERT ... ON CONFLICT DO UPDATE`).
  - Tablas de hechos: `Truncate-and-Load` en un bloque transaccional atómico (`BEGIN ... TRUNCATE ... INSERT ... COMMIT`). Un fallo durante la carga hace `ROLLBACK` y mantiene intacto el estado previo.
- **Reintentos selectivos:** Configurados con `retries=2` únicamente en tareas que interactúan con servicios externos (red/base de datos operacional o DW), y `retries=0` en validaciones y transformaciones determinísticas.

---

## 2. Requerimientos Analíticos (R1 - R3)

| ID | Requerimiento Analítico | Fuentes Utilizadas | KPIs Asociados |
|---|---|---|---|
| **R1** | Comparar la popularidad en Spotify entre artistas con y sin reconocimiento Grammy. | Spotify CSV + Grammy DB | **KPI-1a:** Popularidad media por grupo (con/sin Grammy).<br>**KPI-1b:** Porcentaje del catálogo de pistas con presencia Grammy. |
| **R2** | Identificar géneros que concentran artistas Grammy y contrastar sus perfiles acústicos. | Spotify CSV + Grammy DB | **KPI-2a:** Cantidad y % de artistas Grammy por género.<br>**KPI-2b:** Medias de audio features (`danceability`, `energy`, etc.) por género y grupo. |
| **R3** | Evaluar la relación entre volumen de premios Grammy, presencia en Spotify y popularidad. | Spotify CSV + Grammy DB | **KPI-3a:** # premios, categorías distintas y años por artista.<br>**KPI-3b:** # pistas en catálogo y popularidad promedio (Top 10 histórico). |

---

## 3. Matriz de Trazabilidad Completa

La siguiente matriz detalla el ciclo de vida de los datos, vinculando requerimientos, riesgos de perfilamiento, reglas de calidad, transformaciones, modelo dimensional y KPIs:

| Req. | Riesgo Calidad | Regla DQ | Expectation GX | Transformación (Decisión) | Elemento en DW | Salida / KPI |
|---|---|---|---|---|---|---|
| **R1** | **RK07** (duplicados exactos en Spotify) | **DQ06** (unicidad track-genre) | `ExpectCompoundColumnsToBeUnique(track_id, track_genre, mostly=0.99)` | **T3:** Eliminación de 450 filas duplicadas idénticas. | `dw.dim_track` | KPI-1a, KPI-1b |
| **R1** | **RK15** (14% de popularidad en 0) | **DQ03** (rango [0, 100]), **DQ07** (monitoreo de ceros) | `ExpectColumnValuesToBeBetween(popularity, 0, 100)` | **T12:** Preservación de ceros en fact; exclusión explícita en métrica principal de vista. | `dw.fact_track_credit` (CHECK constraint), `dw.vw_artist_track` | KPI-1a (`avg_popularity_excl_zero`) |
| **R1** | **RK14** (artistas delimitados por `;`) | **DQ05** (artistas no nulos) | `ExpectColumnValuesToNotBeNull(artists, mostly=0.999)` | **T5:** Explode/split de artistas por `;` al grano (pista, artista, género). | `dw.fact_track_credit`, `dw.dim_artist` | KPI-1a, KPI-1b |
| **R1** | **RK22** (carencia de ID compartido entre fuentes) | **DQ14** (clave única de artista), **DQ19** (traslape mínimo) | `ExpectColumnValuesToBeUnique(artist_match_key)`, `ExpectColumnSumToBeBetween(grammy_spotify_overlap, min=30)` | **T9:** Clave de integración normalizada (NFKD unidecode, alfanumérico, casefold). **T10:** Flag `has_grammy_awards`. | `dw.dim_artist.artist_match_key`, `dw.vw_kpi_1a_popularity_by_grammy` | KPI-1a, KPI-1b |
| **R2** | **RK17** (dominio de audio features) | **DQ04**, **DQ18** (features en [0, 1]) | `ExpectColumnValuesToBeBetween(danceability, 0, 1)`, etc. | Validación de límites; rechazo si viola contrato de dominio. | `dw.fact_track_credit` (CHECK constraints en 4 features) | KPI-2b (`vw_kpi_2b_audio_profile`) |
| **R2** | **RK13** (género asignado a nivel de fila) | **DQ01** (schema válido), **DQ15** (grano de créditos) | `ExpectCompoundColumnsToBeUnique(track_id, artist_match_key, genre_name)` | **T6:** Deduplicación al grano estricto de la tabla de hechos. | `dw.dim_genre`, `dw.fact_track_credit` | KPI-2a, KPI-2b |
| **R3** | **RK10** (Grammy sin PK natural) | **DQ09** (unicidad de ID) | `ExpectColumnValuesToBeUnique(source_row_id)` | Asignación de `source_row_id` operacional como clave degenerada `award_id`. | `dw.fact_grammy_award.award_id` | KPI-3a |
| **R3** | **RK11** (100% registros Grammy son ganadores) | **DQ12** (`winner` es True) | `ExpectColumnValuesToBeInSet(winner, [True])` | **T13:** Se descarta columna redundante `winner` y la medida es `award_count = 1`. | `dw.fact_grammy_award.award_count` (CHECK = 1) | KPI-3a, KPI-3b |
| **R3** | **RK05** (38% premios sin artista en origen) | **DQ13** (artista en Grammy), **DQ17** (clave no nula) | `ExpectColumnValuesToNotBeNull(artist, mostly=0.50)` | **T8:** Mapeo a miembro especial `__unknown__` (`artist_key = -1`) para no perder premios. | `dw.dim_artist` (registro -1), `dw.fact_grammy_award` | KPI-3a (filtrado por `artist_type = 'REAL'`) |
| **R3** | **RK23** (colaboraciones en cadena única Grammy) | **DQ16** (grano de hechos de premios) | `ExpectCompoundColumnsToBeUnique(award_id, artist_match_key)` | **T7:** Split condicional de colaboraciones solo con evidencia en Spotify. | `dw.fact_grammy_award` | KPI-3a, KPI-3b |
| **R3** | **RK25** (créditos a nombres genéricos) | **DQ17** (tipo de artista restringido) | `ExpectColumnValuesToBeInSet(artist_type, ['REAL', 'PLACEHOLDER', 'UNKNOWN'])` | **T8:** Mapeo a miembro `__placeholder__` (`artist_key = -2`) para evitar distorsiones en el ranking. | `dw.dim_artist` (registro -2) | KPI-3a/3b (`vw_kpi_3_artist_ranking`) |

---

## 4. Estructura del Repositorio

```
ETL_2026-2_Workshop-2/
├── dags/
│   └── reliable_music_pipeline.py    # DAG de Airflow 3.1.8 (TaskFlow API, gates, reintentos)
├── data/
│   ├── raw/
│   │   ├── spotify_dataset.csv       # Archivo crudo fuente de Spotify
│   │   └── the_grammy_awards.csv     # CSV original usado para inicializar la BD relacional
│   └── staging/                      # Parquet por lote intercambiado entre tareas (ignorado en git)
├── docs/
│   ├── evidence/                     # Evidencias generadas de profiling, transform y validation
│   ├── quality_rules.md              # Catálogo formal de reglas DQ01-DQ20
│   ├── requirements.md               # Definición formal de alcance analítico y requerimientos
│   └── transformation_decisions.md   # Registro de decisiones de ingeniería T1-T13
├── sql/
│   ├── dw_schema.sql                 # DDL del modelo estrella en PostgreSQL (esquema 'dw')
│   ├── kpi_queries.sql               # Vistas SQL analíticas para KPIs de R1, R2 y R3
│   └── source_setup.sql              # DDL de la BD operacional de origen de Grammy
├── src/
│   ├── config.py                     # Configuración de rutas y conexiones DB (híbrido local/Docker)
│   ├── controlled_failure.py         # Generador de datos corruptos para Test B (falla controlada)
│   ├── extract.py                    # Extracción desacoplada hacia staging Parquet
│   ├── load.py                       # Carga a DW: UPSERT en dimensiones y Truncate-and-Load en facts
│   ├── load_grammy_source.py         # Inicialización de la BD fuente operacional (Postgres)
│   ├── run_local_pipeline.py         # Ejecución local de punta a punta (6 etapas)
│   ├── run_validation_checks.py      # Batería de pruebas de validación cruda (escenarios A, B, C)
│   ├── transform.py                  # Lógica de transformación, integración y reconciliación
│   └── validation.py                 # Suites Great Expectations y ejecución de checkpoints
└── requirements.txt                  # Dependencias del proyecto
```

---

## 5. Instrucciones de Ejecución

### Prerrequisitos
- Python 3.12+ con entorno virtual activado.
- PostgreSQL en ejecución (puerto 5433 en host local o 5432 en Docker) con usuario `etl_user`.
- Archivo `.env` configurado en la raíz con las credenciales correspondientes:
  ```env
  ANALYTICS_PG_HOST_PORT=5433
  ANALYTICS_PG_USER=etl_user
  ANALYTICS_PG_PASSWORD=etl_password
  ```

### Paso 1: Configurar el entorno e instalar dependencias
```bash
python -m venv venv
# En Windows PowerShell:
.\venv\Scripts\Activate.ps1
# En Linux/Mac:
source venv/bin/activate

pip install -r requirements.txt
```

### Paso 2: Inicializar la base de datos fuente y el esquema del Data Warehouse
1. Cargar la tabla fuente relacional de Grammy (puerto 5433):
   ```bash
   python src/load_grammy_source.py
   ```
2. Inicializar el esquema dimensional `dw` y las vistas KPI en `music_dw`:
   ```bash
   # Vía psql o ejecutando el script localmente:
   python -c "from src import config; import psycopg2; conn = psycopg2.connect(**config.pg_params('music_dw')); cur = conn.cursor(); cur.execute((config.SQL_DIR / 'dw_schema.sql').read_text(encoding='utf-8')); cur.execute((config.SQL_DIR / 'kpi_queries.sql').read_text(encoding='utf-8')); conn.commit(); conn.close(); print('Esquema y vistas creados exitosamente.')"
   ```

### Paso 3: Ejecución Local de Punta a Punta
Para ejecutar las 6 etapas secuenciales del pipeline (`extract` -> `validate_raw` -> `transform` -> `validate_prepared` -> `load_dw`):
```bash
python -m src.run_local_pipeline
```
*Opcional:* Si desea omitir la escritura al Data Warehouse y solo validar la preparación:
```bash
python -m src.run_local_pipeline --skip-load
```

---

## 6. Ejecución en Apache Airflow (Docker) y Pruebas de Calidad

### Ejecución del DAG en Airflow
1. Iniciar el stack de Airflow (Airflow 3.1.8).
2. Asegurar que la carpeta del proyecto esté montada en `/opt/airflow`.
3. Ingresar a la interfaz web de Airflow (`http://localhost:8080`), habilitar el DAG `reliable_music_pipeline` y ejecutarlo manualmente (**Trigger DAG**).
4. El pipeline completará exitosamente las etapas:
   ```
   extract_spotify  ───► validate_raw_spotify  ──┐
   extract_grammys  ───► validate_raw_grammys  ──┴──► transform_and_integrate ──► validate_prepared ──► load_dw
   ```

### Simulación de Falla Crítica Controlada (Test B)
Para validar que el pipeline es confiable y que las fallas de severidad **Critical** detienen la ejecución protegiendo el Data Warehouse:

1. **Generar el dataset corrupto:**
   Ejecutar el script que inyecta valores fuera de dominio (`popularity = 150`, violando la regla **DQ03**):
   ```bash
   python -m src.controlled_failure
   ```
   *(Crea `data/raw/spotify_bad.csv` sin tocar el archivo original).*

2. **Probar localmente:**
   ```bash
   python -m src.run_local_pipeline --bad
   ```
   *Resultado esperado:* Se lanza `DataQualityError: spotify_raw: Critical rule(s) failed ['DQ03']`, deteniendo el proceso inmediatamente antes de la transformación.

3. **Demostración en Airflow (Captura de pantalla requerida):**
   - En `dags/reliable_music_pipeline.py`, cambiar la variable de configuración:
     ```python
     # SPOTIFY_FILENAME = "spotify_dataset.csv"   # Línea original
     SPOTIFY_FILENAME = "spotify_bad.csv"         # Activar para Test B
     ```
   - Disparar el DAG en la interfaz web de Airflow.
   - La tarea `validate_raw_spotify` fallará y se pondrá en **rojo (failed)**.
   - Las tareas subsecuentes `transform_and_integrate`, `validate_prepared` y `load_dw` quedarán en estado **upstream_failed**, garantizando que ningún dato corrupto ingrese a `music_dw`.
   - Tomar la captura de pantalla en la vista Grid/Graph de Airflow.
   - Restaurar `SPOTIFY_FILENAME = "spotify_dataset.csv"`.

---

## 7. Resultados Analíticos y Verificación de KPIs

Las consultas de `sql/kpi_queries.sql` fueron verificadas directamente contra la base de datos `music_dw`:

### KPI-1a: Popularidad por Grupo de Artista (`dw.vw_kpi_1a_popularity_by_grammy`)
| Grupo de Artistas | Artistas Únicos | Pares Artista-Pista | Popularidad Media Global | Popularidad Media (excl. 0) | % Pistas con Popularidad 0 |
|---|---|---|---|---|---|
| **Non-Grammy artist** | 29,024 | 114,115 | 33.49 | 37.11 | 9.77% |
| **Grammy artist** | 717 | 9,308 | 32.84 | **43.42** | 24.37% |

*Hallazgo clave:* Al excluir las pistas con popularidad 0 (que reflejan ausencia de reproducciones o temas de archivo), los artistas con reconocimiento Grammy tienen en promedio **6.31 puntos más de popularidad** en Spotify que los no premiados.

### KPI-1b: Participación en el Catálogo (`dw.vw_kpi_1b_catalog_share`)
- **Pistas con artista Grammy:** 8,397 de 89,741 pistas únicas (**9.36% del catálogo de Spotify**).

### KPI-2a: Concentración de Artistas Grammy por Género (`dw.vw_kpi_2a_grammy_artists_by_genre`)
- **Top 5 géneros con mayor concentración de artistas premiados:**
  1. **Rock:** 29.18% (75 de 257 artistas)
  2. **Country:** 27.73% (71 de 256 artistas)
  3. **Soul:** 27.22% (86 de 316 artistas)
  4. **Dance:** 21.02% (62 de 295 artistas)
  5. **Jazz:** 19.50% (55 de 282 artistas)

### KPI-3: Top 10 Artistas Más Premiados (`dw.vw_kpi_3_artist_ranking`)
| Artista | Premios Grammy | Categorías Distintas | Período de Premiación | Pistas en Spotify | Popularidad Media Spotify |
|---|---|---|---|---|---|
| **Aretha Franklin** | 18 | 9 | 1967 – 2007 | 8 | 42.00 |
| **Ray Charles** | 18 | 14 | 1960 – 2005 | 9 | 60.29 |
| **U2** | 18 | 9 | 1987 – 2005 | 0 *(fuera de muestra)* | N/A |
| **Tony Bennett** | 18 | 7 | 1962 – 2015 | 17 | 38.67 |
| **Stevie Wonder** | 17 | 9 | 1973 – 2006 | 89 | 20.44 |
| **Vince Gill** | 17 | 6 | 1990 – 2008 | 5 | 1.00 |
| **Beyoncé** | 17 | 13 | 2003 – 2019 | 3 | **72.83** |
| **Jay-Z** | 16 | 7 | 1998 – 2014 | 3 | 53.33 |
| **Alison Krauss** | 16 | 11 | 1990 – 2008 | 8 | 50.75 |
| **B.B. King** | 15 | 7 | 1970 – 2008 | 13 | 55.89 |
