# Descripción general
Este proyecto implementa un pipeline completo de ingesta, normalización, control de calidad, consolidación incremental y evaluación de modelos de matching para listas de sanciones.

El objetivo es simular un motor de screening utilizado en contextos de compliance financiero, permitiendo:

Ingesta de múltiples fuentes (OFAC, UN, EU, PACO, etc.)

Normalización a un esquema canónico común

Aplicación de reglas de calidad de datos

Consolidación incremental con versionado por hash

Generación de base sintética de terceros

Evaluación de modelos de similitud (precision / recall / F1)

# Arquitectura general
El pipeline sigue una arquitectura ETL clásica:

Extract -> Transform -> Quality -> Load (Staging -> Consolidado)

Características clave:

- Base de datos: SQLite (analytics.db)
- Identificación estable por id_registro
- Versionamiento mediante hash_contenido
- Control de cambios mediante hash_anterior
- Proceso incremental por ejecución (run_id)

# Estructura del proyecto
pipeline/
    fuentes/                # Extractores por fuente
    normalizacion/          # Transformación a esquema canónico
    calidad/                # Reglas de calidad de datos
    carga/                  # Staging + Merge incremental
    matching/               # Generación base sintética run_pipeline.py         # Orquestador principal

data/
    terceros_sinteticos.csv

analytics.db
requirements.txt
decisiones.md
README.md

# requisitos
- Python 3.10+
- pip

Instalación del entorno virtual
python -m venv .venv

Activar entorno
- Windows
.\.venv\Scripts\Activate.ps1

- Mac/ Linux
source .venv/bin/activate

Instalar dependencias
pip install -r requirements.txt

# Base de datos
La base analytics.db se crea automáticamente al ejecutar el pipeline.

Tablas principales:

- ingestion_runs
- staging_consolidado
- consolidado

El proceso de merge implementa:

- Inserción de nuevos registros
- Actualización de last_seen_at
- Versionamiento cuando cambia el hash_contenido
- Preservación de hash_anterior
- Auditoría por timestamps

# Ejecutar el pipeline
Desde la raiz del proyecto:
python -m pipeline.run_pipeline

El proceso realiza:
1. Extracción de cada fuente
2. Transformación a esquema canónico
3. Validación con reglas de calidad
4. Carga a staging
5. Merge incremental a consolidado
6. Generación de reporte de ejecución

Se genera un archivo JSON de auditoría en:

reportes/ingesta_<run_id>.json

# Generación de base sintetica de terceros
Para evaluar el motor de matching:
python -m pipeline.matching.generate_terceros

Esto genera:
data/terceros_sinteticos.csv

Composición de la base:

- 9200 registros ficticios (coincidencia = 0)
- 800 registros con coincidencia real (coincidencia = 1):
- Match exacto
- Variación tipográfica (1 carácter diferente o transposición)
- Nombre parcial
- Alias conocido
- Match por número de documento

La columna objetivo para evaluación es:
coincidencia

# Evaluación de modelos de matching
Abrir notebook:
notebooks/matching_models_eval.ipynb

El notebook:

1. Crea o lee terceros_sinteticos.csv
2. Calcula score de similitud contra la tabla consolidado
3. Genera etiqueta predicha (y_pred)
4. Evalúa métricas:
5. Precision
6. Recall
7. F1-score
8. Confusion Matrix
9. Modelos evaluados:
10. Levenshtein
11. Jaro-Winkler
12. Fuzzy similarity

# Politica de Matching
Se implementa enfoque de tres zonas:

- Auto-match → score >= T_high
- Zona gris → T_low ≤ score < T_high
- No match → score < T_low

La calibración de umbrales se realiza usando la base sintética, balanceando:

- Minimización de falsos negativos (riesgo regulatorio)
- Control de carga operativa manual

# Reglas de calidad de datos
Antes de cargar una fuente se aplican reglas como:

- Fuente no puede ser nula
- No permitir fuente vacía post-transform
- Fecha de sanción no puede ser futura
- Hash único por (fuente, origen_id)
- Validación estructura de aliases

Política:

Si una fuente falla reglas críticas no se carga y el pipeline continúa con las demás fuentes.