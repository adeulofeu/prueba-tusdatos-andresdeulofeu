"""pipeline/carga/sqlite_staging.py

Carga de DataFrames/records al staging de SQLite.

Responsabilidades:
- Recibir registros canónicos (dicts o DataFrame)
- Normalizar types problemáticos (NaN, Timestamp)
- Asegurar que aliases/nacionalidad sean listas en memoria
- Serializar listas a JSON TEXT para SQLite
- Insertar por lotes (executemany) con INSERT OR REPLACE

Notas:
- staging_consolidado tiene PK(run_id, id_registro). Usamos OR REPLACE para permitir:
  - re-ejecución idempotente dentro del mismo run (si se repite carga).
- `clear_staging_for_run` borra staging de un run_id (por seguridad).
"""

import json
import math
import sqlite3
from typing import Iterable, Dict, Any, Union

import pandas as pd


def _is_nan(v: Any) -> bool:
    """Detecta NaN (principalmente de pandas)."""
    if isinstance(v, float):
        try:
            return math.isnan(v)
        except Exception:
            return False
    return False


def _clean(v: Any):
    """Normaliza valores para SQLite:
    - NaN -> None
    - Timestamp -> ISO string
    - otros -> sin cambios
    """
    if _is_nan(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime().isoformat()
    return v


def _ensure_list(x: Any):
    """Asegura que aliases/nacionalidad sean listas en memoria.

    Acepta:
      - list -> ok
      - None/NaN -> []
      - str JSON de lista -> intenta parsear
    Si no se puede, retorna [].

    Importante:
    - Esto evita que la misma fila tenga hash diferente por cambios de representación.
    """
    if x is None or _is_nan(x):
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return []
        # si viene JSON (por compatibilidad), lo intentamos parsear
        try:
            parsed = json.loads(s)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _json_text(x):
    """Serializa una lista a JSON para persistirla en TEXT."""
    if x is None:
        return None
    return json.dumps(x, ensure_ascii=False)


def clear_staging_for_run(conn: sqlite3.Connection, run_id: str):
    """Borra staging de un run_id (útil antes de recargar)."""
    conn.execute("DELETE FROM staging_consolidado WHERE run_id = ?", (run_id,))
    conn.commit()


def _iter_records_from_df(df: pd.DataFrame) -> Iterable[Dict[str, Any]]:
    """Convierte un DF canónico a records dict limpios.

    Pasos:
    1) Copia DF para no mutar el original
    2) Normaliza aliases/nacionalidad a listas si existen
    3) Normaliza activo a bool (y luego a 0/1 al persistir)
    4) Reemplaza NaN -> None (en todo el DF)
    5) Itera dict records, limpiando valores finales

    Motivo:
    - pandas usa NaN y tipos numpy que SQLite no maneja bien.
    """
    df2 = df.copy()

    # Normalizar columnas list-like (si existen)
    if "aliases" in df2.columns:
        df2["aliases"] = df2["aliases"].map(_ensure_list)
    if "nacionalidad" in df2.columns:
        df2["nacionalidad"] = df2["nacionalidad"].map(_ensure_list)

    # Activo debe ser bool (si viene NaN o None -> False)
    if "activo" in df2.columns:
        df2["activo"] = df2["activo"].map(lambda x: bool(x) if x is not None and not _is_nan(x) else False)

    # Reemplazar NaN -> None en todo el DF
    # Nota: where mantiene valores; mask para nan
    df2 = df2.where(pd.notna(df2), None)

    # Records
    for r in df2.to_dict("records"):
        # limpieza final por si queda algún NaN raro
        yield {k: _clean(v) for k, v in r.items()}


def load_to_staging(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    records: Union[Iterable[Dict], pd.DataFrame],
    batch_size: int = 1000
) -> int:
    """
    Inserta registros en staging_consolidado.
    """
    sql = """
    INSERT OR REPLACE INTO staging_consolidado (
      run_id, id_registro, fuente, tipo_sujeto, nombres, apellidos, aliases, fecha_nacimiento,
      nacionalidad, numero_documento, tipo_sancion, fecha_sancion, fecha_vencimiento,
      activo, fecha_ingesta, origen_id, hash_contenido
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    # Si llega DF, lo convertimos a records limpios
    if isinstance(records, pd.DataFrame):
        records_iter = _iter_records_from_df(records)
    else:
        records_iter = records

    cur = conn.cursor()
    batch = []
    total = 0

    for r in records_iter:
        # Limpieza ligera (por compatibilidad)
        r = {k: _clean(v) for k, v in r.items()}

        # Validaciones mínimas de esquema: id_registro y hash son obligatorios
        rid = r.get("id_registro")
        h = r.get("hash_contenido")
        if not rid or not h:
            raise ValueError("Record must include id_registro and hash_contenido")

        # Serializamos listas a JSON TEXT
        aliases_list = _ensure_list(r.get("aliases"))
        nac_list = _ensure_list(r.get("nacionalidad"))

        batch.append((
            run_id,
            rid,
            r.get("fuente"),
            r.get("tipo_sujeto"),
            r.get("nombres"),
            r.get("apellidos"),
            _json_text(aliases_list),
            r.get("fecha_nacimiento"),
            _json_text(nac_list),
            r.get("numero_documento"),
            r.get("tipo_sancion"),
            r.get("fecha_sancion"),
            r.get("fecha_vencimiento"),
            1 if bool(r.get("activo")) else 0,
            r.get("fecha_ingesta"),
            r.get("origen_id"),
            h
        ))

        # Flush por lotes (evita un executemany gigante)
        if len(batch) >= batch_size:
            cur.executemany(sql, batch)
            conn.commit()
            total += len(batch)
            batch.clear()

    # Insert final si quedó algo
    if batch:
        cur.executemany(sql, batch)
        conn.commit()
        total += len(batch)

    return total