import json
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

Issue = Dict[str, Any]
Rule = Callable[[pd.DataFrame], List[Issue]]


CANON_COLS = [
    "id_registro",
    "fuente",
    "tipo_sujeto",
    "nombres",
    "apellidos",
    "aliases",
    "fecha_nacimiento",
    "nacionalidad",
    "numero_documento",
    "tipo_sancion",
    "fecha_sancion",
    "fecha_vencimiento",
    "activo",
    "fecha_ingesta",
    "origen_id",
    "hash_contenido",
]


def _issue(level: str, rule: str, message: str, sample: Optional[Dict[str, Any]] = None) -> Issue:
    return {"level": level, "rule": rule, "message": message, "sample": sample or {}}


def _is_blank_scalar(x: Any) -> bool:
    """Blank para un valor escalar (no Series)."""
    return x is None or (isinstance(x, str) and x.strip() == "")


def _first_bad_row(df: pd.DataFrame, mask: pd.Series, cols: List[str]) -> Dict[str, Any]:
    """
    Toma la primera fila donde mask=True y devuelve un sample dict con las columnas pedidas.
    Si no hay filas, retorna {}.
    """
    bad = df.loc[mask, cols]
    if bad.empty:
        return {}
    return bad.iloc[0].to_dict()


def _parse_date_to_pydate(value: Any) -> Optional[date]:
    """
    Convierte a date si es parseable.
    Acepta:
      - date / datetime
      - string ISO (YYYY-MM-DD o ISO con hora)
    """
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return datetime.fromisoformat(s).date()
        except ValueError:
            return None
    return None


# -------------------------
# Reglas de calidad (DF)
# -------------------------

def q_non_empty(df: pd.DataFrame) -> List[Issue]:
    """La fuente no puede quedar con 0 registros post-transform."""
    if df is None or len(df) == 0:
        return [_issue("ERROR", "non_empty", "La fuente no puede quedar con 0 registros tras la transformación.", {})]
    return []


def q_fuente_not_null(df: pd.DataFrame) -> List[Issue]:
    """Ningún registro debe tener fuente nula/vacía."""
    if "fuente" not in df.columns:
        return [_issue("ERROR", "fuente_not_null", "Falta columna 'fuente' en el DataFrame canónico.", {})]

    s = df["fuente"]
    mask = s.isna() | (s.astype(str).str.strip() == "")
    if mask.any():
        sample = _first_bad_row(df, mask, ["id_registro", "fuente", "origen_id"])
        return [_issue("ERROR", "fuente_not_null", "Ningún registro debe tener 'fuente' nula/vacía.", sample)]
    return []


def q_required_columns_present(df: pd.DataFrame, required: List[str] = CANON_COLS) -> List[Issue]:
    """
    Garantiza que el DF cumple el contrato de columnas canónicas.
    Esto NO es “data quality” de negocio, pero te salva de bugs silenciosos.
    """
    missing = [c for c in required if c not in df.columns]
    if missing:
        return [_issue("ERROR", "required_columns_present", f"Faltan columnas canónicas: {missing}", {})]
    return []


def q_aliases_is_list(df: pd.DataFrame) -> List[Issue]:
    """
    aliases debe ser lista en memoria (antes de serializar a JSON para DB).
    Se acepta:
      - list
      - None / NaN (lo tratamos como ok, luego lo normalizas a [])
    Se rechaza:
      - string
      - dict
      - otros tipos
    """
    if "aliases" not in df.columns:
        return [_issue("ERROR", "aliases_is_list", "Falta columna 'aliases' en el DataFrame canónico.", {})]

    def bad_type(x: Any) -> bool:
        if x is None:
            return False
        # pandas puede meter NaN float; lo aceptamos
        if isinstance(x, float) and pd.isna(x):
            return False
        return not isinstance(x, list)

    mask = df["aliases"].map(bad_type)
    if mask.any():
        sample = _first_bad_row(df, mask, ["id_registro", "aliases", "origen_id"])
        sample["aliases_type"] = type(sample.get("aliases")).__name__
        return [_issue("ERROR", "aliases_is_list", "'aliases' debe ser lista (en memoria), no string/dict.", sample)]
    return []


def q_nacionalidad_is_list(df: pd.DataFrame) -> List[Issue]:
    """nacionalidad debe ser lista (mismo criterio que aliases)."""
    if "nacionalidad" not in df.columns:
        return [_issue("ERROR", "nacionalidad_is_list", "Falta columna 'nacionalidad' en el DataFrame canónico.", {})]

    def bad_type(x: Any) -> bool:
        if x is None:
            return False
        if isinstance(x, float) and pd.isna(x):
            return False
        return not isinstance(x, list)

    mask = df["nacionalidad"].map(bad_type)
    if mask.any():
        sample = _first_bad_row(df, mask, ["id_registro", "nacionalidad", "origen_id"])
        sample["nacionalidad_type"] = type(sample.get("nacionalidad")).__name__
        return [_issue("ERROR", "nacionalidad_is_list", "'nacionalidad' debe ser lista (en memoria).", sample)]
    return []


def q_fecha_sancion_not_future(df: pd.DataFrame) -> List[Issue]:
    """
    fecha_sancion no puede ser futura.
    - Si no parsea, la regla NO falla (eso es otra regla: 'fecha_sancion_parseable' si la quieres).
    """
    if "fecha_sancion" not in df.columns:
        return [_issue("ERROR", "fecha_sancion_not_future", "Falta columna 'fecha_sancion' en el DataFrame canónico.", {})]

    today = date.today()

    def is_future(x: Any) -> bool:
        d = _parse_date_to_pydate(x)
        return (d is not None) and (d > today)

    mask = df["fecha_sancion"].map(is_future)
    if mask.any():
        sample = _first_bad_row(df, mask, ["id_registro", "fecha_sancion", "origen_id", "fuente"])
        return [_issue("ERROR", "fecha_sancion_not_future", "'fecha_sancion' no puede ser futura.", sample)]
    return []


def q_hash_unique_by_fuente_origen(df: pd.DataFrame) -> List[Issue]:
    """
    hash_contenido debe ser único por (fuente, origen_id).
    OJO: esta regla solo aplica si origen_id viene lleno. Si origen_id es None/vacío, la ignoramos.
    """
    needed = ["fuente", "origen_id", "hash_contenido"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        return [_issue("ERROR", "hash_unique_by_fuente_origen_id", f"Faltan columnas requeridas: {missing}", {})]

    # Filtramos filas donde fuente/origen_id/hash estén presentes
    f = df["fuente"].astype(str).str.strip()
    o = df["origen_id"].astype(str).str.strip()
    h = df["hash_contenido"].astype(str).str.strip()

    # Nota: cuando origen_id es None, astype(str) lo vuelve "None" => lo evitamos usando isna() también
    valid = (~df["fuente"].isna()) & (f != "") & (~df["origen_id"].isna()) & (o != "") & (~df["hash_contenido"].isna()) & (h != "")
    dfx = df.loc[valid, ["fuente", "origen_id", "hash_contenido", "id_registro"]].copy()

    if dfx.empty:
        return []

    # Si para una misma (fuente, origen_id) hay más de un hash distinto => error
    nunique_hash = dfx.groupby(["fuente", "origen_id"])["hash_contenido"].nunique()
    bad_keys = nunique_hash[nunique_hash > 1]
    if not bad_keys.empty:
        fuente, origen_id = bad_keys.index[0]
        sample_rows = dfx[(dfx["fuente"] == fuente) & (dfx["origen_id"] == origen_id)].head(2)
        sample = {
            "fuente": fuente,
            "origen_id": origen_id,
            "hashes": sample_rows["hash_contenido"].tolist(),
            "id_registro_sample": sample_rows["id_registro"].tolist(),
        }
        return [_issue("ERROR", "hash_unique_by_fuente_origen_id",
                       "hash_contenido debe ser único por (fuente, origen_id).",
                       sample)]
    return []


# -------------------------
# Runner
# -------------------------

DEFAULT_RULES: List[Rule] = [
    q_non_empty,
    q_required_columns_present,
    q_fuente_not_null,
    q_aliases_is_list,
    q_nacionalidad_is_list,
    q_fecha_sancion_not_future,
    q_hash_unique_by_fuente_origen,
]


def validations(df: pd.DataFrame, rules: List[Rule] = DEFAULT_RULES, hard_fail: bool = True) -> List[Issue]:
    """
    Ejecuta reglas de calidad sobre un DataFrame canónico.

    hard_fail=True:
      - si hay al menos un ERROR, levanta ValueError con resumen
    """
    issues: List[Issue] = []
    for rule in rules:
        issues.extend(rule(df))

    if hard_fail:
        errors = [i for i in issues if i.get("level") == "ERROR"]
        if errors:
            msg = "; ".join([f'{e["rule"]}: {e["message"]}' for e in errors])
            raise ValueError(f"Quality checks failed: {msg}")

    return issues