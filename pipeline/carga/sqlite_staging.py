import json
import math
import sqlite3
from typing import Iterable, Dict, Any, Union

import pandas as pd


def _is_nan(v: Any) -> bool:
    # NaN (pandas) -> True
    if isinstance(v, float):
        try:
            return math.isnan(v)
        except Exception:
            return False
    return False


def _clean(v: Any):
    # NaN (pandas) -> None
    if _is_nan(v):
        return None
    # pandas Timestamp -> isoformat
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime().isoformat()
    return v


def _ensure_list(x: Any):
    """
    En memoria, aliases/nacionalidad deben ser list.
    Acepta:
      - list -> ok
      - None/NaN -> []
      - str JSON de lista -> parsea y devuelve list (si se puede)
      - cualquier otro -> [] (y lo deja para QC si quieres endurecer)
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
    if x is None:
        return None
    return json.dumps(x, ensure_ascii=False)


def clear_staging_for_run(conn: sqlite3.Connection, run_id: str):
    conn.execute("DELETE FROM staging_consolidado WHERE run_id = ?", (run_id,))
    conn.commit()


def _iter_records_from_df(df: pd.DataFrame) -> Iterable[Dict[str, Any]]:
    """
    Convierte un DF canónico a records dict limpios (NaN->None),
    asegurando que aliases/nacionalidad sean listas.
    """
    # convertimos a objetos python para evitar tipos numpy raros
    df2 = df.copy()

    # Normalizar columnas list-like (si existen)
    if "aliases" in df2.columns:
        df2["aliases"] = df2["aliases"].map(_ensure_list)
    if "nacionalidad" in df2.columns:
        df2["nacionalidad"] = df2["nacionalidad"].map(_ensure_list)

    # activo debe ser bool (si viene NaN o None -> False)
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

    Ahora soporta:
      - records: Iterable[Dict] (modo anterior)
      - records: pd.DataFrame canónico (modo nuevo)
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

        rid = r.get("id_registro")
        h = r.get("hash_contenido")
        if not rid or not h:
            raise ValueError("Record must include id_registro and hash_contenido")

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

        if len(batch) >= batch_size:
            cur.executemany(sql, batch)
            conn.commit()
            total += len(batch)
            batch.clear()

    if batch:
        cur.executemany(sql, batch)
        conn.commit()
        total += len(batch)

    return total