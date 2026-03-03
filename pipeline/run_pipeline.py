"""pipeline/run_pipeline.py

Orquestador principal del pipeline de ingesta.

Este script ejecuta, en orden:
1) Inicialización de directorios y base de datos (todas las tablas via schema.py)
2) Registro de la ejecución (ingestion_runs)
3) Extracción por fuente (descarga/obtención del "raw")
4) Transformación a esquema canónico + Quality Checks + carga a staging (por run_id)
5) Merge final staging -> consolidado (tabla productiva)
6) Generación de reporte JSON en disco (auditoría / evidencia)
7) Monitoreo post-run (métricas + alertas) usando el JSON + DB

Diseño:
- Cada fuente reporta: status, paths y conteos en `by_source`.
- `errors` acumula excepciones por fuente y etapa.
- En caso de fallas parciales se usa status `SUCCESS_WITH_ERRORS`.
- El monitoreo NO rompe el pipeline (se considera "non-blocking" para la prueba).
"""

import os
import time
import uuid
import json
import logging
import sqlite3
from pathlib import Path
from dotenv import load_dotenv

from pipeline.fuentes.un_xml import extract_un
from pipeline.fuentes.eu_xml import extract_eu
from pipeline.fuentes.paco_siri_zip import extract_paco_siri
from pipeline.fuentes.paco_fgn_csv import extract_paco_fgn
from pipeline.fuentes.worldbank_scraper import extract_worldbank
from pipeline.fuentes.ofac_sdn_xml import extract_ofac_sdn
from pipeline.normalizacion.sdn_ofac_transform import transform_ofac_sdn
from pipeline.normalizacion.un_transform import transform_un_consolidated
from pipeline.normalizacion.paco_fgn_transform import transform_paco_fgn
from pipeline.normalizacion.paco_siri_transform import transform_paco_siri
from pipeline.carga.schema import init_all_tables
from pipeline.carga.sqlite_staging import clear_staging_for_run, load_to_staging
from pipeline.carga.sqlite_merge import merge_staging_to_consolidado
from pipeline.calidad.quality import validations
from pipeline.monitoring.monitor import run_monitoring

# Cargar variables desde .env (local dev)
load_dotenv()

# Variables de entorno
ENV = os.getenv("ENV", "local")
DB_PATH = os.getenv("DB_PATH", "analytics.db")

# Nivel de logs: DEBUG/INFO/WARNING/ERROR
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Paths del proyecto
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reportes"
DB_FILE = PROJECT_ROOT / DB_PATH

# Función para crear id de pipeline
def make_run_id():
    """Genera un identificador único y ordenable por tiempo para la ejecución."""
    return time.strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]

# Función para creación del logger
def setup_logger():
    """Crea un logger de consola sencillo, con formato consistente."""
    logger = logging.getLogger("pipeline")
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    
    # Evitar duplicar handlers si se reimporta el módulo
    if not logger.handlers:
        ch = logging.StreamHandler()
        fmt = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    return logger

# Función para verificar las existincia de las carpetas de reportes
def ensure_dirs():
    """Crea carpetas base del proyecto si no existen."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Función para inicialización y creación de tabla de ejecucción
def init_db(conn):
    """Inicializa la DB creando TODAS las tablas.

    Importante:
    - La fuente de verdad del DDL es schema.py (init_all_tables).
    - Evitamos repetir CREATE TABLE aquí para mantener consistencia.
    """
    init_all_tables(conn)
    conn.commit()

def run_quality(df, source_key: str, by_source: dict, errors: dict, hard_fail: bool = True):
    """
    Ejecuta quality checks sobre el DF canónico.
    Guarda issues en by_source[source_key].
    Si hard_fail=True y hay ERROR, levanta excepción.
    """
    issues = validations(df, hard_fail=hard_fail)

    # Guardar un resumen liviano en el reporte
    by_source.setdefault(source_key, {})
    by_source[source_key]["quality_issues"] = issues
    by_source[source_key]["quality_error_count"] = sum(1 for i in issues if i.get("level") == "ERROR")
    by_source[source_key]["quality_issue_count"] = len(issues)

    return issues

def _merge_durations(durations: dict, inc: dict):
    """Acumula (suma) métricas de tiempo por etapa.

    Esto permite que extractores/transformadores reporten sus timings individuales
    y el pipeline tenga un resumen total por run.
    """
    for k, v in (inc or {}).items():
        durations[k] = round(durations.get(k, 0) + float(v), 4)


def _set_source_failed(by_source: dict, errors: dict, source_key: str, stage: str, exc: Exception):
    by_source.setdefault(source_key, {})["status"] = "FAILED"
    errors.setdefault(source_key, []).append({"stage": stage, "error": str(exc)})


def _run_transform_quality_load(
    *,
    conn,
    run_id: str,
    logger,
    source_key: str,
    by_source: dict,
    errors: dict,
    durations: dict,
    # extractor info dict ya cargado en by_source[source_key]
    raw_path_value: str,
    transform_fn,
    transform_returns_meta: bool,
    # nombres de métricas en durations
    dur_tf_key: str,
    dur_ld_key: str,
    # política QC
    qc_hard_fail: bool = True,
) -> None:
    """Ejecuta el bloque estándar: Transform -> Quality -> Load (staging).

    Parámetros clave:
    - raw_path_value: string que viene del extractor (path del archivo RAW).
    - transform_fn: función que convierte RAW a DF canónico.
    - transform_returns_meta: si la función retorna (df, meta) o solo df.
    - qc_hard_fail: si True, cualquier ERROR de quality evita cargar a staging.

    Efectos:
    - Actualiza `by_source[source_key]` con conteos, calidad, meta, etc.
    - Escribe errores en `errors[source_key]` con etapa y mensaje.
    """

    # Si no hay path, skip
    if not raw_path_value:
        by_source.setdefault(source_key, {})["transform_status"] = "SKIPPED"
        return

    try:
        raw_path = Path(raw_path_value)

        # 1. Transformación
        t_tf = time.time()
        if transform_returns_meta:
            df, meta = transform_fn(raw_path)
        else:
            df = transform_fn(raw_path)
            meta = {}
        durations[dur_tf_key] = round(time.time() - t_tf, 4)

        # 2. Quality (si falla, no cargamos y seguimos)
        try:
            issues = validations(df, hard_fail=qc_hard_fail)

            by_source.setdefault(source_key, {})
            by_source[source_key]["quality_issue_count"] = len(issues)
            by_source[source_key]["quality_error_count"] = sum(1 for i in issues if i.get("level") == "ERROR")
            by_source[source_key]["quality_warn_count"] = sum(1 for i in issues if i.get("level") == "WARN")

            # Guardar issues completos
            by_source[source_key]["quality_issues"] = issues

        except Exception as qe:
            # Política: NO carga esta fuente y sigue
            errors.setdefault(source_key, []).append({"stage": "quality", "error": str(qe)})
            by_source.setdefault(source_key, {})["transform_status"] = "FAILED_QUALITY"
            by_source[source_key].update(meta)
            return

        # 3. Carga a staging
        t_ld = time.time()
        n_stg = load_to_staging(conn, run_id=run_id, records=df)
        durations[dur_ld_key] = round(time.time() - t_ld, 4)

        # 4. Reporte por fuente
        by_source.setdefault(source_key, {})
        by_source[source_key]["transform_status"] = "OK"
        by_source[source_key]["staging_records"] = n_stg
        by_source[source_key]["records_out"] = int(len(df))
        by_source[source_key].update(meta)

    except Exception as e:
        # Cualquier excepción aquí es transform o load inesperado
        logger.exception(f"{source_key} transform/load failed")
        errors.setdefault(source_key, []).append({"stage": "transform_load_staging", "error": str(e)})
        by_source.setdefault(source_key, {})["transform_status"] = "FAILED"

# Main
def main():
    logger = setup_logger()
    ensure_dirs()

    # Identificadores de auditoría
    run_id = make_run_id()
    started_at = time.strftime("%Y-%m-%d %H:%M:%S")
    run_date = time.strftime("%Y%m%d")
    logger.info(f"Starting run_id={run_id} env={ENV}")
    
    # Estado global del run (se consolida al final)
    status = "SUCCESS"
    notes = None

    # Estructuras que terminan en el reporte JSON
    durations = {}
    by_source = {}
    errors = {}
    finished_at = None

    # Abrimos conexión a SQLite para staging/merge/control
    conn = sqlite3.connect(DB_FILE)

    try:
        # 1. Inicialización de pipeline
        t0 = time.time()
        init_db(conn)
        durations["init_db_sec"] = round(time.time() - t0, 4)

        conn.execute(
            "INSERT INTO ingestion_runs(run_id, env, started_at, status) VALUES (?, ?, ?, ?)",
            (run_id, ENV, started_at, "RUNNING")
        )
        conn.commit()
        
        # Limpieza de la tabla de stagging para la nueva iteracción
        clear_staging_for_run(conn, run_id)

        # 2. Extracción de datos
        extract_jobs = [
            ("OFAC_SDN", extract_ofac_sdn),
            ("UN", extract_un),
            ("EU", extract_eu),
            ("PACO_SIRI", extract_paco_siri),
            ("PACO_FGN", extract_paco_fgn),
            # ("WORLDBANK", extract_worldbank),
        ]

        for source_key, extractor in extract_jobs:
            try:
                # extractor retorna: (info, durs, errs)
                info, durs, errs = extractor(DATA_DIR, logger)
                by_source[source_key] = info
                _merge_durations(durations, durs)

                if errs:
                    # Errores no necesariamente rompen el run, pero lo marcan con warnings
                    errors[source_key] = errs
                    status = "SUCCESS_WITH_ERRORS"

            except Exception as e:
                # Fallo inesperado del extractor
                logger.exception(f"{source_key} extract failed")
                by_source[source_key] = {"status": "FAILED"}
                errors[source_key] = [{"stage": "extract", "error": str(e)}]
                status = "SUCCESS_WITH_ERRORS"

        # 3. Proceso de TRANSFORM + QUALITY + LOAD (sin duplicación)
        # Cada item define:
        # - source_key: llave en by_source
        # - path_key: key donde el extractor guardó el path del raw
        # - transform_fn: función de transformación
        # - transform_returns_meta: si retorna (df, meta) o solo df
        # - dur keys
        transform_jobs = [
            {
                "source_key": "OFAC_SDN",
                "path_key": "raw_xml_path",
                "transform_fn": transform_ofac_sdn,
                "transform_returns_meta": True,
                "dur_tf_key": "ofac_transform_sec",
                "dur_ld_key": "ofac_load_staging_sec",
            },
            {
                "source_key": "UN",
                "path_key": "raw_path",
                "transform_fn": transform_un_consolidated,
                "transform_returns_meta": True,
                "dur_tf_key": "un_transform_sec",
                "dur_ld_key": "un_load_staging_sec",
            },
            {
                "source_key": "PACO_FGN",
                "path_key": "raw_csv_path",
                "transform_fn": transform_paco_fgn,
                "transform_returns_meta": False,
                "dur_tf_key": "paco_fgn_transform_sec",
                "dur_ld_key": "paco_fgn_load_staging_sec",
            },
            {
                "source_key": "PACO_SIRI",
                "path_key": "extracted_file_path",
                "transform_fn": transform_paco_siri,
                "transform_returns_meta": True,
                "dur_tf_key": "paco_siri_transform_sec",
                "dur_ld_key": "paco_siri_load_staging_sec",
            },
        ]

        for job in transform_jobs:
            source_key = job["source_key"]

            # Política: solo transformamos si extraction quedó OK
            if by_source.get(source_key, {}).get("status") != "OK":
                by_source.setdefault(source_key, {})["transform_status"] = "SKIPPED"
                continue

            raw_path_value = by_source.get(source_key, {}).get(job["path_key"])

            _run_transform_quality_load(
                conn=conn,
                run_id=run_id,
                logger=logger,
                source_key=source_key,
                by_source=by_source,
                errors=errors,
                durations=durations,
                raw_path_value=raw_path_value,
                transform_fn=job["transform_fn"],
                transform_returns_meta=job["transform_returns_meta"],
                dur_tf_key=job["dur_tf_key"],
                dur_ld_key=job["dur_ld_key"],
                qc_hard_fail=True,  # Política: si QC falla, no carga esa fuente y sigue
            )

            # Si alguna fuente tuvo problemas (extract errs o quality errs), marcamos SUCCESS_WITH_ERRORS
            if source_key in errors:
                status = "SUCCESS_WITH_ERRORS"

        # 4. Merge de la data a la tabla de productiva
        try:
            t_mg = time.time()
            merge_staging_to_consolidado(conn, run_id)
            durations["merge_final_sec"] = round(time.time() - t_mg, 4)
            logger.info(f"Final merge OK for run_id={run_id}")
        except Exception as e:
            # Si el merge falla, la ejecución se considera FAILED
            logger.exception("Final merge failed")
            status = "FAILED"
            notes = f"Final merge failed: {str(e)}"

    except Exception as e:
        # Fallo global no capturado por secciones anteriores
        status = "FAILED"
        notes = str(e)
        logger.exception("Pipeline failed")

    finally:
        # 5. Cierre de ejecución: actualizar ingestion_runs
        try:
            conn.execute(
                "UPDATE ingestion_runs SET finished_at=?, status=?, notes=? WHERE run_id=?",
                (finished_at, status, notes, run_id),
            )
            conn.commit()
        except Exception as e:
            # Si esto falla, lo reportamos pero intentamos cerrar igual
            logger.exception(f"Could not update ingestion_runs in finally: {e}")
        finally:
            conn.close()

    # 6. Reporte JSON (auditoría) - se escribe en disco
    report = {
        "run_id": run_id,
        "env": ENV,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": status,
        "durations_sec": durations,
        "by_source": by_source,
        "errors": errors
    }

    report_path = REPORTS_DIR / f"ingesta_{run_id}.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info(f"Report written: {report_path}")
    
    # 7. Monitoring (métricas + alertas + linaje)
    try:
        mon = run_monitoring(db_path=str(DB_FILE), report_path=str(report_path), logger=logger, channel="console")
        logger.info(f"Monitoring done: {mon}")
    except Exception as e:
        # No queremos que monitoreo rompa el pipeline en esta prueba
        logger.exception(f"Monitoring failed (non-blocking): {e}")

    logger.info("Done.")

if __name__ == "__main__":
    main()