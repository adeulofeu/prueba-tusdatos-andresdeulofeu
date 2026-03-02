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
from pipeline.carga.schema import init_staging_and_consolidado
from pipeline.carga.sqlite_staging import clear_staging_for_run, load_to_staging
from pipeline.carga.sqlite_merge import merge_staging_to_consolidado
from pipeline.calidad.quality import validations

load_dotenv()

# Variables de entorno
ENV = os.getenv("ENV", "local")
DB_PATH = os.getenv("DB_PATH", "analytics.db")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
UN_XML_URL = os.getenv("UN_XML_URL", "").strip()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reportes"
DB_FILE = PROJECT_ROOT / DB_PATH

# Función para crear id de pipeline
def make_run_id():
    return time.strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]

# Función para creación del logger
def setup_logger():
    logger = logging.getLogger("pipeline")
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    if not logger.handlers:
        ch = logging.StreamHandler()
        fmt = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    return logger

# Función para verificar las existincia de las carpetas de reportes
def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Función para inicialización y creación de tabla de ejecucción
def init_db(conn):
    ddl = """
    CREATE TABLE IF NOT EXISTS ingestion_runs (
        run_id TEXT PRIMARY KEY,
        env TEXT NOT NULL,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        status TEXT NOT NULL,
        notes TEXT
    );
    """
    conn.executescript(ddl)
    init_staging_and_consolidado(conn)
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
    """Suma durations parciales dentro del dict global."""
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
    """
    Pipeline genérico para una fuente:
      - transform -> df (+ meta opcional)
      - quality -> si falla: NO carga y sigue
      - load_to_staging(df)
    """

    # Si no hay path, skip
    if not raw_path_value:
        by_source.setdefault(source_key, {})["transform_status"] = "SKIPPED"
        return

    try:
        raw_path = Path(raw_path_value)

        # 1) Transformación
        t_tf = time.time()
        if transform_returns_meta:
            df, meta = transform_fn(raw_path)
        else:
            df = transform_fn(raw_path)
            meta = {}
        durations[dur_tf_key] = round(time.time() - t_tf, 4)

        # 2) Quality (si falla, no cargamos y seguimos)
        try:
            issues = validations(df, hard_fail=qc_hard_fail)

            by_source.setdefault(source_key, {})
            by_source[source_key]["quality_issue_count"] = len(issues)
            by_source[source_key]["quality_error_count"] = sum(1 for i in issues if i.get("level") == "ERROR")
            by_source[source_key]["quality_warn_count"] = sum(1 for i in issues if i.get("level") == "WARN")

            # (opcional) guarda issues completos (pueden ser grandes)
            by_source[source_key]["quality_issues"] = issues

        except Exception as qe:
            # Política: NO carga esta fuente y sigue
            errors.setdefault(source_key, []).append({"stage": "quality", "error": str(qe)})
            by_source.setdefault(source_key, {})["transform_status"] = "FAILED_QUALITY"
            by_source[source_key].update(meta)
            return

        # 3) Carga a staging (ahora tu load_to_staging acepta df)
        t_ld = time.time()
        n_stg = load_to_staging(conn, run_id=run_id, records=df)
        durations[dur_ld_key] = round(time.time() - t_ld, 4)

        # 4) Reporte por fuente
        by_source.setdefault(source_key, {})
        by_source[source_key]["transform_status"] = "OK"
        by_source[source_key]["staging_records"] = n_stg
        by_source[source_key]["records_out"] = int(len(df))
        by_source[source_key].update(meta)

    except Exception as e:
        logger.exception(f"{source_key} transform/load failed")
        errors.setdefault(source_key, []).append({"stage": "transform_load_staging", "error": str(e)})
        by_source.setdefault(source_key, {})["transform_status"] = "FAILED"

# Main
def main():
    logger = setup_logger()
    ensure_dirs()

    run_id = make_run_id()
    started_at = time.strftime("%Y-%m-%d %H:%M:%S")
    run_date = time.strftime("%Y%m%d")
    logger.info(f"Starting run_id={run_id} env={ENV}")

    status = "SUCCESS"
    notes = None
    durations = {}
    by_source = {}
    errors = {}
    finished_at = None
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
                info, durs, errs = extractor(DATA_DIR, logger)
                by_source[source_key] = info
                _merge_durations(durations, durs)

                if errs:
                    errors[source_key] = errs
                    status = "SUCCESS_WITH_ERRORS"

            except Exception as e:
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
                "transform_returns_meta": False,  # en tu versión actual retorna solo df
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

            # Solo corre si la extracción quedó OK
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
            logger.exception("Final merge failed")
            status = "FAILED"
            notes = f"Final merge failed: {str(e)}"

    except Exception as e:
        status = "FAILED"
        notes = str(e)
        logger.exception("Pipeline failed")

    finally:
        finished_at = time.strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "UPDATE ingestion_runs SET finished_at=?, status=?, notes=? WHERE run_id=?",
            (finished_at, status, notes, run_id)
        )
        conn.commit()
        conn.close()

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
    logger.info("Done.")


if __name__ == "__main__":
    main()