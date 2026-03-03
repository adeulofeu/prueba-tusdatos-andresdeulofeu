"""
pipeline/monitoring/monitor.py

Monitoreo del pipeline basado en:
- DB (control plane): ingestion_runs, staging_consolidado, consolidado
- JSON report (data plane): by_source, durations_sec, errors, etc.

Este módulo asume que TODAS las tablas ya existen porque se crean en schema.py
(init_all_tables) cuando se inicializa la DB.

Qué hace:
1) Lee el reporte JSON del run.
2) Persiste métricas por fuente en ingestion_run_metrics.
3) Evalúa reglas y genera alertas (>=5) en la tabla alerts.
4) Emite notificación mock (console) y permite linaje via explain_alert().
"""

import json
import sqlite3
import uuid
from pathlib import Path
from datetime import datetime, timedelta


# ----------------------------
# Utils
# ----------------------------
def _safe_int(x):
    try:
        if x is None or x == "":
            return None
        return int(x)
    except Exception:
        return None


def _safe_float(x):
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def _parse_dt(s):
    """
    ingestion_runs.started_at / finished_at se guardan como texto.
    Normalmente vienen como 'YYYY-MM-DD HH:MM:SS'
    """
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None


def _utc_now_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


# ----------------------------
# Alerts persistence + notify
# ----------------------------
def insert_alert(
    conn,
    run_id,
    source_key,
    alert_type,
    severity,
    channel,
    threshold,
    value,
    message,
    context,
):
    """
    Inserta una alerta en tabla alerts.
    - threshold/value se guardan como texto (para simplicidad y auditoría).
    - context_json guarda el "por qué" (paths, errors, etc.)
    """
    alert_id = uuid.uuid4().hex[:12]
    conn.execute(
        """
        INSERT INTO alerts(
          alert_id, run_id, source_key, alert_type, severity, channel,
          threshold, value, message, context_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            alert_id,
            run_id,
            source_key,
            alert_type,
            severity,
            channel,
            None if threshold is None else str(threshold),
            None if value is None else json.dumps(value, ensure_ascii=False)
            if isinstance(value, (dict, list))
            else str(value),
            message,
            json.dumps(context or {}, ensure_ascii=False),
            _utc_now_str(),
        ),
    )
    return alert_id


def notify_stub(logger, channel, msg):
    """
    Notificación mock: para la prueba técnica basta log/print.
    """
    if logger is not None:
        logger.warning(f"[ALERT:{channel}] {msg}")
    else:
        print(f"[ALERT:{channel}] {msg}")


# ----------------------------
# Metrics persistence
# ----------------------------
def persist_metrics_from_report(conn, report):
    """
    Persiste métricas por fuente en ingestion_run_metrics.
    Lee principalmente report['by_source'].
    """
    run_id = report.get("run_id")
    by_source = report.get("by_source") or {}

    for source_key, info in by_source.items():
        records_out = _safe_int(info.get("records_out"))
        staging_records = _safe_int(info.get("staging_records"))

        q_issue = _safe_int(info.get("quality_issue_count"))
        q_err = _safe_int(info.get("quality_error_count"))
        q_warn = _safe_int(info.get("quality_warn_count"))

        extract_status = info.get("status")  # OK / FAILED / SKIPPED...
        transform_status = info.get("transform_status")  # OK / FAILED_QUALITY / FAILED...

        # Matching metrics (si existen en el JSON; si no, quedan NULL)
        avg_match_score = _safe_float(info.get("avg_match_score"))
        match_rate = _safe_float(info.get("match_rate"))
        matches_count = _safe_int(info.get("matches_count"))

        conn.execute(
            """
            INSERT OR REPLACE INTO ingestion_run_metrics(
              run_id, source_key,
              records_out, staging_records,
              quality_issue_count, quality_error_count, quality_warn_count,
              extract_status, transform_status,
              avg_match_score, match_rate, matches_count,
              created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                source_key,
                records_out,
                staging_records,
                q_issue,
                q_err,
                q_warn,
                extract_status,
                transform_status,
                avg_match_score,
                match_rate,
                matches_count,
                _utc_now_str(),
            ),
        )


# ----------------------------
# Alert rules (>=5)
# ----------------------------
def generate_alerts(
    conn,
    report,
    channel="console",
    stale_days=2,
    change_pct_threshold=0.60,
    duration_factor=2.0,
    min_runtime_seconds_for_spike=60.0,
):
    """
    Genera alertas basadas en:
    - Estado global del run (report['status'])
    - Estado por fuente (by_source[*].status / transform_status)
    - Conteos (records_out / staging_records / quality errors)
    - Histórico (ingestion_runs + ingestion_run_metrics)
    """
    run_id = report.get("run_id")
    run_status = report.get("status")  # SUCCESS / SUCCESS_WITH_ERRORS / FAILED
    durations = report.get("durations_sec") or {}
    by_source = report.get("by_source") or {}
    errors = report.get("errors") or {}

    # ALERTA 1: Run no limpio
    if run_status in ("FAILED", "SUCCESS_WITH_ERRORS"):
        insert_alert(
            conn=conn,
            run_id=run_id,
            source_key=None,
            alert_type="RUN_NOT_CLEAN",
            severity="CRITICAL" if run_status == "FAILED" else "WARN",
            channel=channel,
            threshold="status==SUCCESS",
            value=run_status,
            message=f"Pipeline terminó con estado {run_status}",
            context={"run_id": run_id},
        )

    # ALERTA 2: Fuente falló en extracción (status FAILED)
    # ALERTA 3: Fuente falló en transform / quality (transform_status FAILED / FAILED_QUALITY)
    for source_key, info in by_source.items():
        src_status = info.get("status")
        src_transform = info.get("transform_status")

        if src_status == "FAILED":
            insert_alert(
                conn=conn,
                run_id=run_id,
                source_key=source_key,
                alert_type="SOURCE_EXTRACT_FAILED",
                severity="CRITICAL",
                channel=channel,
                threshold="status==OK",
                value=src_status,
                message=f"{source_key} falló en extracción",
                context={"source_info": info, "errors": errors.get(source_key, [])},
            )

        if src_transform in ("FAILED", "FAILED_QUALITY"):
            severity = "CRITICAL" if src_transform == "FAILED" else "WARN"
            insert_alert(
                conn=conn,
                run_id=run_id,
                source_key=source_key,
                alert_type="SOURCE_TRANSFORM_FAILED",
                severity=severity,
                channel=channel,
                threshold="transform_status==OK",
                value=src_transform,
                message=f"{source_key} falló en transform/quality: {src_transform}",
                context={"source_info": info, "errors": errors.get(source_key, [])},
            )

    # ALERTA 4: Fuente devolvió 0 registros (records_out o staging_records)
    for source_key, info in by_source.items():
        rec_out = _safe_int(info.get("records_out"))
        stg = _safe_int(info.get("staging_records"))
        if (rec_out is not None and rec_out == 0) or (stg is not None and stg == 0):
            insert_alert(
                conn=conn,
                run_id=run_id,
                source_key=source_key,
                alert_type="ZERO_RECORDS",
                severity="WARN",
                channel=channel,
                threshold=">0",
                value={"records_out": rec_out, "staging_records": stg},
                message=f"{source_key} produjo 0 registros (records_out/staging_records)",
                context={"source_info": info},
            )

    # ALERTA 5: Calidad con errores (si quality_error_count alto)
    # (Regla simple: si hay errores de calidad > 0, alertar)
    for source_key, info in by_source.items():
        q_err = _safe_int(info.get("quality_error_count"))
        if q_err is not None and q_err > 0:
            insert_alert(
                conn=conn,
                run_id=run_id,
                source_key=source_key,
                alert_type="QUALITY_ERRORS_PRESENT",
                severity="WARN",
                channel=channel,
                threshold="quality_error_count==0",
                value=q_err,
                message=f"{source_key} tiene errores de calidad: {q_err}",
                context={"source_info": info},
            )

    # ALERTA 6: Fuente stale (no se actualiza en N días)
    # Definimos "actualizó" como: último run con extract_status OK y transform_status OK
    for source_key in by_source.keys():
        row = conn.execute(
            """
            SELECT r.finished_at
            FROM ingestion_run_metrics m
            JOIN ingestion_runs r ON r.run_id = m.run_id
            WHERE m.source_key = ?
              AND m.extract_status = 'OK'
              AND m.transform_status = 'OK'
              AND r.status IN ('SUCCESS', 'SUCCESS_WITH_ERRORS')
              AND r.finished_at IS NOT NULL AND TRIM(r.finished_at) <> ''
            ORDER BY r.finished_at DESC
            LIMIT 1
            """,
            (source_key,),
        ).fetchone()

        if row and row[0]:
            last_dt = _parse_dt(row[0])
            if last_dt:
                if datetime.now() - last_dt > timedelta(days=stale_days):
                    insert_alert(
                        conn=conn,
                        run_id=run_id,
                        source_key=source_key,
                        alert_type="SOURCE_STALE",
                        severity="WARN",
                        channel=channel,
                        threshold=f">{stale_days}d",
                        value=row[0],
                        message=f"{source_key} no se actualiza hace más de {stale_days} días (último: {row[0]})",
                        context={"last_success_finished_at": row[0]},
                    )

    # ALERTA 7: Cambio abrupto de registros vs run anterior (por fuente)
    # Compara records_out vs el run anterior exitoso (offset 1)
    for source_key, info in by_source.items():
        curr = _safe_int(info.get("records_out"))
        if curr is None:
            continue

        prev_row = conn.execute(
            """
            SELECT m.records_out
            FROM ingestion_run_metrics m
            JOIN ingestion_runs r ON r.run_id = m.run_id
            WHERE m.source_key = ?
              AND m.records_out IS NOT NULL
              AND r.status IN ('SUCCESS', 'SUCCESS_WITH_ERRORS')
              AND r.finished_at IS NOT NULL AND TRIM(r.finished_at) <> ''
            ORDER BY r.finished_at DESC
            LIMIT 1 OFFSET 1
            """,
            (source_key,),
        ).fetchone()

        if prev_row and prev_row[0] is not None:
            prev = int(prev_row[0])
            if prev > 0:
                pct = abs(curr - prev) / prev
                if pct >= change_pct_threshold:
                    insert_alert(
                        conn=conn,
                        run_id=run_id,
                        source_key=source_key,
                        alert_type="RECORDS_CHANGE_SPIKE",
                        severity="WARN",
                        channel=channel,
                        threshold=f">={change_pct_threshold*100:.0f}%",
                        value={"prev": prev, "curr": curr, "pct": round(pct, 4)},
                        message=f"{source_key} cambio abrupto records_out: prev={prev} curr={curr} ({pct:.1%})",
                        context={"prev": prev, "curr": curr, "pct": pct},
                    )

    # ALERTA 8 (extra): Duración anormal (aprox)
    # Usamos suma de durations_sec como proxy del runtime
    curr_runtime = 0.0
    for v in durations.values():
        if isinstance(v, (int, float)):
            curr_runtime += float(v)

    # Regla simple para prueba: si supera factor * min_runtime_seconds_for_spike
    if curr_runtime > (min_runtime_seconds_for_spike * duration_factor):
        insert_alert(
            conn=conn,
            run_id=run_id,
            source_key=None,
            alert_type="RUN_DURATION_SPIKE",
            severity="WARN",
            channel=channel,
            threshold=f">{min_runtime_seconds_for_spike * duration_factor:.0f}s",
            value=round(curr_runtime, 2),
            message=f"Duración total aproximada alta: {curr_runtime:.1f}s",
            context={"durations_sec": durations},
        )


# ----------------------------
# Lineage (alert -> run -> source -> staging sample)
# ----------------------------
def explain_alert(conn, alert_id):
    """
    Linaje básico y demostrable:
    alert_id -> (run_id, source_key) -> staging_consolidado sample -> (origen_id, hash_contenido, id_registro)

    Devuelve un dict para que lo puedas imprimir como JSON en la demo.
    """
    row = conn.execute(
        """
        SELECT run_id, source_key, alert_type, severity, channel, message, context_json, created_at
        FROM alerts
        WHERE alert_id=?
        """,
        (alert_id,),
    ).fetchone()

    if not row:
        return {"error": "alert_id not found", "alert_id": alert_id}

    run_id, source_key, alert_type, severity, channel, message, context_json, created_at = row
    ctx = {}
    if context_json:
        try:
            ctx = json.loads(context_json)
        except Exception:
            ctx = {"raw_context_json": context_json}

    lineage = {
        "alert_id": alert_id,
        "created_at": created_at,
        "alert_type": alert_type,
        "severity": severity,
        "channel": channel,
        "message": message,
        "run_id": run_id,
        "source_key": source_key,
        "context": ctx,
    }

    # Traer info del run (ingestion_runs)
    run_row = conn.execute(
        """
        SELECT env, started_at, finished_at, status, notes
        FROM ingestion_runs
        WHERE run_id=?
        """,
        (run_id,),
    ).fetchone()

    if run_row:
        lineage["run"] = {
            "env": run_row[0],
            "started_at": run_row[1],
            "finished_at": run_row[2],
            "status": run_row[3],
            "notes": run_row[4],
        }

    # Sample de staging para rastrear hasta origen
    if source_key:
        samples = conn.execute(
            """
            SELECT fuente, origen_id, hash_contenido, id_registro, fecha_ingesta
            FROM staging_consolidado
            WHERE run_id=? AND fuente=?
            LIMIT 5
            """,
            (run_id, source_key),
        ).fetchall()

        lineage["staging_samples"] = [
            {
                "fuente": s[0],
                "origen_id": s[1],
                "hash_contenido": s[2],
                "id_registro": s[3],
                "fecha_ingesta": s[4],
            }
            for s in samples
        ]

    return lineage


# ----------------------------
# Entry point
# ----------------------------
def run_monitoring(db_path, report_path, logger=None, channel="console"):
    """
    Ejecuta monitoreo para un run:
    - Lee reporte JSON desde disco
    - Inserta métricas por fuente
    - Genera alertas con reglas
    - Emite notificación mock
    """
    report_path = Path(report_path)
    report = json.loads(report_path.read_text(encoding="utf-8"))

    conn = sqlite3.connect(db_path)
    try:
        persist_metrics_from_report(conn, report)
        generate_alerts(conn, report, channel=channel)
        conn.commit()

        # Notificación: listar alertas generadas para este run
        rows = conn.execute(
            """
            SELECT alert_id, alert_type, severity, source_key, message
            FROM alerts
            WHERE run_id=?
            ORDER BY created_at
            """,
            (report.get("run_id"),),
        ).fetchall()

        for alert_id, typ, sev, src, msg in rows:
            notify_stub(
                logger,
                channel,
                f"{sev} {typ} source={src} alert_id={alert_id} :: {msg}",
            )

        return {"run_id": report.get("run_id"), "alerts_count": len(rows)}

    finally:
        conn.close()