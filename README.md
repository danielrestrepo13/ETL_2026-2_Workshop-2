\# Workshop 2: Building a Reliable Batch Data Pipeline

\*\*Asignatura:\*\* ETL \- Ingeniería de Datos e Inteligencia Artificial (2026-2)    
\*\*Universidad:\*\* Universidad Autónoma de Occidente (UAO)    
\*\*Stack tecnológico:\*\* Python 3.12, PostgreSQL 16, Apache Airflow 3.1.8, Great Expectations 1.23.x, Docker & Docker Compose.

\---

\#\# 1\. Problema y Objetivo Analítico  
El objetivo de este proyecto es construir un pipeline de datos batch confiable, resiliente e idempotente que integra dos fuentes heterogéneas para alimentar un Data Warehouse analítico en PostgreSQL (\`music\_dw\`). El objetivo de negocio es responder preguntas sobre la relación entre el reconocimiento en los premios Grammy y la popularidad o características acústicas de las canciones en Spotify.

\---

\#\# 2\. Requerimientos Analíticos (R1 \- R3)

| ID | Requerimiento Analítico | Fuentes Utilizadas | KPIs Asociados |  
|---|---|---|---|  
| \*\*R1\*\* | Comparar la popularidad en Spotify entre artistas con y sin reconocimiento Grammy. | Spotify CSV \+ Grammy DB | \*\*KPI-1a:\*\* Popularidad media por grupo.\<br\>\*\*KPI-1b:\*\* Porcentaje del catálogo de pistas con presencia Grammy. |  
| \*\*R2\*\* | Identificar géneros que concentran artistas Grammy y contrastar sus perfiles acústicos. | Spotify CSV \+ Grammy DB | \*\*KPI-2a:\*\* Cantidad y % de artistas Grammy por género.\<br\>\*\*KPI-2b:\*\* Medias de audio features por género y grupo. |  
| \*\*R3\*\* | Evaluar la relación entre volumen de premios Grammy, presencia en Spotify y popularidad. | Spotify CSV \+ Grammy DB | \*\*KPI-3a:\*\* \# premios, categorías y años por artista.\<br\>\*\*KPI-3b:\*\* \# pistas en catálogo y popularidad promedio. |

\---

\#\# 3\. Fuentes de Datos  
1\. \*\*Spotify Dataset (\`data/raw/spotify\_dataset.csv\`):\*\* Catálogo de 114,000 pistas con atributos de audio (\`danceability\`, \`energy\`, etc.), \`popularity\`, \`artists\` y \`track\_genre\`.  
2\. \*\*Grammy Awards Source DB (\`grammy\_source.public.grammy\_awards\`):\*\* Base de datos relacional operacional en PostgreSQL (4,810 registros históricos). \*\*Se extrae exclusivamente mediante consulta SQL\*\*, cumpliendo el requisito 4.3.

\---

\#\# 4\. Arquitectura del Pipeline y Orquestación

El pipeline sigue una arquitectura desacoplada y está orquestado por \*\*Apache Airflow 3.1.8\*\* utilizando la \*\*TaskFlow API\*\* (\`@dag\`, \`@task\`).

\`\`\`text  
       \+-----------------------+              \+------------------------------------+  
       |  Spotify Tracks CSV   |              | Grammy Awards (PostgreSQL Source)  |  
       \+-----------+-----------+              \+-----------------+------------------+  
                   |                                            |  
                   v                                            v  
         \[ extract\_spotify \]                          \[ extract\_grammys \]  
                   |                                            |  
                   v                                            v  
       \[ validate\_raw\_spotify \]                     \[ validate\_raw\_grammys \]  
       (Great Expectations Gate)                    (Great Expectations Gate)  
                   \\                                            /  
                    \\                                          /  
                     v                                        v  
                 \+-----------------------------------------------+  
                 |            transform\_and\_integrate           |  
                 \+-----------------------+-----------------------+  
                                         |  
                                         v  
                            \[ validate\_prepared \]  
                          (Great Expectations Gate)  
                                         |  
                                         v  
                                  \[ load\_dw \]  
                     (Safe Rerun: UPSERT dims \+ Truncate-and-Load facts)  
                                         |  
                                         v  
                 \+-----------------------------------------------+  
                 |       PostgreSQL Data Warehouse (music\_dw)    |  
                 \+-----------------------------------------------+  
\`\`\`

\---

\#\# 5\. Hallazgos de Perfilamiento de Datos  
El perfilamiento (\`notebooks/data\_profiling.ipynb\`) reveló evidencia clave que guio la limpieza:  
\* \*\*Duplicidad (Spotify):\*\* Se encontraron 450 filas exactamente duplicadas al mismo nivel de grano.  
\* \*\*Calidad de Dominio (Spotify):\*\* El 14% de las pistas registra una popularidad de \`0\`.   
\* \*\*Completitud (Grammy):\*\* El 38% de los premios históricos carecen de un artista principal explícito.  
\* \*\*Consistencia:\*\* Los artistas en Spotify están anidados y delimitados por \`;\`, requiriendo un \*explode\* para la integración.

\---

\#\# 6\. Riesgos y Catálogo de Reglas de Calidad (DQ)  
Se diseñaron 20 reglas automatizadas para mitigar los riesgos identificados:

| ID Regla | Dimensión | Descripción de la Regla | Severidad |  
|---|---|---|---|  
| \*\*DQ01 \- DQ02\*\* | Validez | Esquemas crudos coinciden con la definición esperada. | Critical |  
| \*\*DQ03\*\* | Validez | Rango de popularidad estrictamente entre \[0, 100\]. | Critical |  
| \*\*DQ04\*\* | Validez | Audio features acústicos estrictamente entre \[0, 1\]. | Critical |  
| \*\*DQ05\*\* | Completitud | Columna \`artists\` no nula (tolerancia \> 99.9%). | Warning |  
| \*\*DQ06\*\* | Unicidad | Unicidad compuesta por track y género. | Warning |  
| \*\*DQ07\*\* | Consistencia | Monitoreo de porcentaje de pistas con popularidad 0\. | Informational |  
| \*\*DQ08 \- DQ12\*\* | Varios | Reglas operacionales fuente Grammy (ID único, fechas válidas). | Critical / Warn |  
| \*\*DQ13\*\* | Completitud | Artista Grammy presente en al menos el 50% de las filas. | Warning |  
| \*\*DQ14 \- DQ20\*\* | Integración | Reglas sobre datos preparados (llaves únicas, grano resuelto). | Critical |

\---

\#\# 7\. Diseño de Great Expectations (Validación Automatizada)  
El pipeline utiliza \*\*Great Expectations 1.23.x\*\* en dos compuertas (Raw Gate y Prepared Gate):  
\* \*\*Datasources & Batch Definitions:\*\* Configurados de forma efímera para evaluar los DataFrames extraídos y transformados en memoria mediante Pandas o Parquet.  
\* \*\*Expectation Suites:\*\* Se agruparon las reglas DQ en suites independientes (\`spotify\_raw\_suite\`, \`grammy\_raw\_suite\`, \`prepared\_dim\_artist\`, etc.).  
\* \*\*Checkpoints:\*\* Evalúan el lote contra la suite. Las fallas de severidad \*Critical\* lanzan una excepción que detiene el DAG, mientras que las \*Warning\*/\*Informational\* se registran pero permiten continuar.

\---

\#\# 8\. Estrategia de Transformación e Integración  
\* \*\*Limpieza:\*\* Deduplicación estricta de las 450 filas de Spotify (T3).  
\* \*\*Normalización de Llaves (Integration Contract):\*\* Los nombres de artistas se limpian mediante NFKD unidecode, alfanumérico y casefold (T9) para lograr el cruce entre Spotify y Grammy.  
\* \*\*Miembros Especiales:\*\* Se crearon llaves subrogadas \`-1\` (\`\_\_unknown\_\_\`) para los premios sin artista y \`-2\` (\`\_\_placeholder\_\_\`) para créditos genéricos (T8).  
\* \*\*Grano:\*\* Split de colaboraciones delimitadas por \`;\` para respetar el grano atómico (T5).

\---

\#\# 9\. Modelo Dimensional (Star Schema)  
El Data Warehouse (\`music\_dw.dw\`) está estructurado bajo un modelo en estrella, soportando los KPIs requeridos.

\`\`\`mermaid  
erDiagram  
    fact\_track\_credit }|--|| dim\_artist : "artist\_key"  
    fact\_track\_credit }|--|| dim\_track : "track\_key"  
    fact\_track\_credit }|--|| dim\_genre : "genre\_key"  
    fact\_grammy\_award }|--|| dim\_artist : "artist\_key"  
    fact\_grammy\_award }|--|| dim\_category : "category\_key"  
    fact\_grammy\_award }|--|| dim\_year : "year\_key"  
\`\`\`

\---

\#\# 10\. Política de Fallos y Reintentos

| Condición | Severidad | Respuesta del Pipeline | ¿Reintento? | Justificación |  
|---|---|---|---|---|  
| Fallo red/BD | Operacional | Intenta reconectar. | Sí (\`retries=2\`) | Problema transitorio solucionable sin cambios en código. |  
| Falla GX crítica | Critical | Tarea pasa a \`Failed\`, detiene DAG. | No (\`retries=0\`) | Falla determinística de calidad; datos inválidos no deben entrar. |  
| Falla GX menor | Warning / Info | Tarea pasa a \`Success\`, registra alerta. | No aplica | No compromete la estructura del Data Warehouse. |

\---

\#\# 11\. Evidencias de Confiabilidad (Registro de Evidencias)

| ID Evidencia | Ejecución / Tarea | Artefacto o Ruta | Qué demuestra |  
|---|---|---|---|  
| \*\*E-01\*\* | \`validate\_raw\` | \`docs/evidence/validation/A\_baseline/\` | Aprobación de Quality Gates en ejecución exitosa. |  
| \*\*E-02\*\* | \`validate\_raw\_spotify\` | \`docs/evidence/airflow/test\_b\_failure.png\` | Bloqueo efectivo del DAG ante datos corruptos (DQ03). |  
| \*\*E-03\*\* | \`transform\_integrate\` | \`docs/evidence/transform/\*/reconciliation.json\` | Invariantes de transformación e integridad mantenidos. |  
| \*\*E-04\*\* | DAG Airflow | \`docs/evidence/airflow/test\_a\_success.png\` | Orquestación correcta y estado en verde. |  
| \*\*E-05\*\* | \`load\_dw\` | \`docs/evidence/rerun/rerun\_comparison.md\` | Idempotencia y repetibilidad segura sin duplicados. |

\---

\#\# 12\. Estrategia de Repetibilidad (Safe Rerun)  
Para garantizar la \*\*idempotencia\*\* (Requisito 7.3), el módulo \`load\_dw\` implementa un \`UPSERT\` (\`ON CONFLICT DO UPDATE\`) para dimensiones y un \`Truncate-and-Load\` en un bloque transaccional atómico para las tablas de hechos. 

\*\*Demostración de Rerun:\*\*  
| Entidad | Filas Antes del Rerun | Filas Después del Rerun | Resultado |  
|---|---|---|---|  
| \`dim\_artist\` | 30,763 | 30,763 | Mantiene unicidad |  
| \`fact\_track\_credit\` | 157,531 | 157,531 | Sin duplicados silenciosos |

\---

\#\# 13\. Dashboard Analítico y KPIs

Se crearon vistas materializadas (\`sql/kpi\_queries.sql\`) consultadas directamente desde Power BI.  
\* \*\*KPI-1a:\*\* Popularidad media excluyendo ceros es de \*\*43.42\*\* (Con Grammy) vs \*\*37.11\*\* (Sin Grammy).  
\* \*\*KPI-1b:\*\* \*\*9.36%\*\* del catálogo tiene participación de un artista galardonado.  
\* \*\*KPI-2a:\*\* Rock, Country y Soul concentran el top 3 histórico de ganadores.

\*\*Visualizaciones en Power BI:\*\* \*\[En proceso \- dashboard pbix en desarrollo conectando a \`localhost:5433\`\]\*

\---

\#\# 14\. Matriz de Trazabilidad End-to-End

| Req. | Riesgo Calidad | Regla DQ | Expectation GX | Transformación | Elemento DW | KPI |  
|---|---|---|---|---|---|---|  
| \*\*R1\*\* | \*\*RK07\*\* (duplicados) | \*\*DQ06\*\* (unicidad) | \`ExpectCompoundColumnsToBeUnique\` | \*\*T3:\*\* Drop 450 filas duplicadas. | \`dim\_track\` | KPI-1a |  
| \*\*R1\*\* | \*\*RK15\*\* (14% pop 0\) | \*\*DQ03\*\*, \*\*DQ07\*\* | \`ExpectColumnValuesToBeBetween\` | \*\*T12:\*\* Ceros mantenidos, excluidos en vista. | \`fact\_track\_credit\` | KPI-1a |  
| \*\*R2\*\* | \*\*RK13\*\* (género por fila) | \*\*DQ15\*\* (grano) | \`ExpectCompoundColumnsToBeUnique\` | \*\*T6:\*\* Deduplicación estricta hechos. | \`dim\_genre\` | KPI-2a |  
| \*\*R3\*\* | \*\*RK05\*\* (38% sin artista)| \*\*DQ13\*\*, \*\*DQ17\*\* | \`ExpectColumnValuesToNotBeNull\` | \*\*T8:\*\* Uso de llave \`-1\` \`\_\_unknown\_\_\`. | \`dim\_artist\` | KPI-3a |

\---

\#\# 15\. Supuestos y Limitaciones  
\* \*\*Integración por Nombre Normalizado:\*\* Al no existir un ID global, la integración asume que cadenas normalizadas idénticas (NFKD) pertenecen a la misma entidad.  
\* \*\*Popularidad Cero:\*\* El significado real de \`popularity \= 0\` no se establece en los metadatos de Spotify; se asume matemáticamente como ausencia de reproducciones o temas de archivo, por lo que se excluye de las medias comparativas.  
\* \*\*Refinamiento RK25:\*\* Se detectaron 93 nombres con paréntesis; 69 resultaron ser placeholders genéricos (ej. 'Various Artists') mapeados a llave \`-2\`, el resto eran entidades válidas.  
\* \*\*Compartición de Metadata:\*\* \`extract\_grammys\` hereda el flujo solo para obtener el \`batch\_id\` de forma ordenada, evitando usar XCom masivo.

\---

\#\# 16\. Configuración y Ejecución (Setup)

\*\*1. Entorno (.env):\*\*  
\`\`\`env  
ANALYTICS\_PG\_HOST\_PORT=5433  
ANALYTICS\_PG\_USER=etl\_user  
ANALYTICS\_PG\_PASSWORD=etl\_password  
\`\`\`

\*\*2. Ejecución Local:\*\*  
\`\`\`bash  
python \-m venv venv  
.\\venv\\Scripts\\Activate.ps1  
pip install \-r requirements.txt  
python src/load\_grammy\_source.py  
python \-m src.run\_local\_pipeline  
\`\`\`

\*\*3. Ejecución en Docker (Airflow):\*\*  
\`\`\`bash  
docker compose build  
docker compose up airflow-init  
docker compose up \-d  
\`\`\`  
Acceda a \`localhost:8080\`, habilite \`reliable\_music\_pipeline\` y presione \*\*Trigger DAG\*\*.  
\`\`\`