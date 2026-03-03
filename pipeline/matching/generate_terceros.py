""" scripts/generate_terceros.py

Generador de base sintética de terceros (para evaluar modelos de matching).

Salida:
- data/terceros_sinteticos.csv con columna 'coincidencia' como label (0/1).

Casos generados:
- ~9200 ficticios (no match)
- ~800 matches distribuidos en: exact, typo, partial, alias, doc

Nota:
- Se apoya en consolidado para sembrar matches reales.

"""

import os
import json
import random
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Config
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = os.getenv("DB_PATH", "analytics.db")
DB_FILE = PROJECT_ROOT / DB_PATH
CONSOLIDADO_TABLE = "consolidado"

OUT_DIR = Path("data")
OUT_CSV = OUT_DIR / "terceros_sinteticos.csv"

N_FICTICIOS = 9200
N_PER_CASE = 160
SEED = 42

random.seed(SEED)

TIPOS_DOC = ["CC", "CE", "PASAPORTE", "NIT"]
PAISES = ["COLOMBIA", "VENEZUELA", "MEXICO", "PERU", "ECUADOR", "USA", "ESPAÑA", "BRASIL", "ARGENTINA"]
NACIONALIDADES = ["COLOMBIANA", "VENEZOLANA", "MEXICANA", "PERUANA", "ECUATORIANA", "ESTADOUNIDENSE", "ESPAÑOLA", "BRASILEÑA", "ARGENTINA"]

NOMBRES_BASE = ["JUAN", "ANDRES", "MARIA", "LUISA", "CARLOS", "ANA", "DANIEL", "LAURA", "JORGE", "CAMILA", "DAVID", "SARA"]
APELLIDOS_BASE = ["PEREZ", "GOMEZ", "RODRIGUEZ", "MARTINEZ", "HERNANDEZ", "LOPEZ", "GARCIA", "SANCHEZ", "TORRES", "RAMIREZ"]

# Helpers
def _json_load_list(x: Any) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return []
        try:
            v = json.loads(s)
            return v if isinstance(v, list) else []
        except Exception:
            return []
    return []

def _first_token(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    parts = [p for p in str(s).strip().split() if p]
    return parts[0] if parts else None

def _partial_name(nombres: Optional[str], apellidos: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    return _first_token(nombres), _first_token(apellidos)

def _one_char_typo(s: Optional[str]) -> Optional[str]:
    """1 char diferente o transposición de letras (para simular typo)."""
    if not s:
        return s
    s = str(s).strip()
    if len(s) < 2:
        return s

    if random.random() < 0.5:
        i = random.randrange(len(s))
        ch = s[i]
        if ch.isalpha():
            base = ord("A")
            new_ch = chr(base + ((ord(ch.upper()) - base + 1) % 26))
        elif ch.isdigit():
            new_ch = str((int(ch) + 1) % 10)
        else:
            new_ch = "X"
        return s[:i] + new_ch + s[i+1:]
    else:
        i = random.randrange(len(s) - 1)
        return s[:i] + s[i+1] + s[i] + s[i+2:]

def _infer_tipo_documento(numero_documento: Optional[str], tipo_sujeto: str) -> Optional[str]:
    if not numero_documento:
        return None
    if tipo_sujeto == "PERSONA_JURIDICA":
        return "NIT"
    if any(c.isalpha() for c in str(numero_documento)):
        return "PASAPORTE"
    return "CC"

def _pick_nacionalidad() -> str:
    return random.choice(NACIONALIDADES)

def _pick_pais_residencia() -> str:
    return random.choice(PAISES)

def _random_fecha_nacimiento() -> Optional[str]:
    # rango simple 1950-2005
    y = random.randint(1950, 2005)
    m = random.randint(1, 12)
    d = random.randint(1, 28)
    return f"{y:04d}-{m:02d}-{d:02d}"

def _random_doc(tipo_sujeto: str) -> Tuple[Optional[str], Optional[str]]:
    if tipo_sujeto == "PERSONA_JURIDICA":
        # NIT ficticio
        nit = str(random.randint(100000000, 999999999))
        return nit, "NIT"
    tipo_doc = random.choice(["CC", "CE", "PASAPORTE"])
    if tipo_doc == "PASAPORTE":
        doc = f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.randint(1000000, 9999999)}"
    else:
        doc = str(random.randint(10000000, 1999999999))
    return doc, tipo_doc

def _row_to_synthetic_schema(r: Dict[str, Any], id_tercero: str) -> Dict[str, Any]:
    tipo_sujeto = (r.get("tipo_sujeto") or "PERSONA_NATURAL").strip()
    nombres = r.get("nombres")
    apellidos = r.get("apellidos")
    fecha_nacimiento = r.get("fecha_nacimiento")
    nacionalidad = r.get("nacionalidad")

    # En tu consolidado suele venir nacionalidad como JSON list
    nac_list = _json_load_list(nacionalidad)
    nacionalidad_out = nac_list[0] if nac_list else (nacionalidad.strip() if isinstance(nacionalidad, str) and nacionalidad.strip() else None)

    numero_documento = r.get("numero_documento")
    tipo_documento = _infer_tipo_documento(numero_documento, tipo_sujeto)
    pais_residencia = nacionalidad_out or _pick_pais_residencia()

    return {
    "id_tercero": id_tercero,
    "tipo_sujeto": tipo_sujeto,
    "nombres": nombres,
    "apellidos": apellidos,
    "fecha_nacimiento": fecha_nacimiento,
    "nacionalidad": nacionalidad_out,
    "numero_documento": numero_documento,
    "tipo_documento": tipo_documento,
    "pais_residencia": pais_residencia,
    "coincidencia": 1,   # MATCH REAL
    }

# DB queries
def _fetch_random_rows(conn: sqlite3.Connection, n: int) -> List[Dict[str, Any]]:
    sql = f"""
    SELECT tipo_sujeto, nombres, apellidos, fecha_nacimiento, nacionalidad, numero_documento, aliases
    FROM {CONSOLIDADO_TABLE}
    ORDER BY RANDOM()
    LIMIT ?;
    """
    cur = conn.execute(sql, (n,))
    cols = [d[0] for d in cur.description]
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]

def _fetch_rows_with_doc(conn: sqlite3.Connection, n: int) -> List[Dict[str, Any]]:
    sql = f"""
    SELECT tipo_sujeto, nombres, apellidos, fecha_nacimiento, nacionalidad, numero_documento, aliases
    FROM {CONSOLIDADO_TABLE}
    WHERE numero_documento IS NOT NULL AND TRIM(numero_documento) <> ''
    ORDER BY RANDOM()
    LIMIT ?;
    """
    cur = conn.execute(sql, (n,))
    cols = [d[0] for d in cur.description]
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]

def _fetch_rows_with_alias(conn: sqlite3.Connection, n: int) -> List[Dict[str, Any]]:
    # Traemos más porque no todos tendrán aliases útiles
    raw = _fetch_random_rows(conn, n * 10)
    out = []
    for r in raw:
        aliases = _json_load_list(r.get("aliases"))
        if aliases:
            out.append(r)
        if len(out) >= n:
            break
    return out

# Generación
def _name_key(tipo_sujeto: str, nombres: Optional[str], apellidos: Optional[str]) -> str:
    # clave estable para evitar duplicados de nombres (no usa id)
    n = (nombres or "").strip().upper()
    a = (apellidos or "").strip().upper()
    if tipo_sujeto == "PERSONA_JURIDICA":
        return f"J|{n}"
    return f"N|{n}|{a}"


def _gen_ficticios(n: int, start_idx: int, seen_name_keys: set) -> List[Dict[str, Any]]:
    rows = []

    SUFIJOS_EMP = ["SAS", "LTDA", "S.A.", "S.A.S.", "S. EN C.", "CIA"]
    TAGS_EMP = ["COMERCIAL", "SERVICIOS", "INVERSIONES", "LOGISTICA", "HOLDING", "GROUP", "TRADING"]

    i = 0
    attempts = 0
    max_attempts = n * 50  # evita loop infinito si el espacio fuera pequeño

    while i < n and attempts < max_attempts:
        attempts += 1

        tipo_sujeto = "PERSONA_NATURAL" if random.random() < 0.85 else "PERSONA_JURIDICA"

        if tipo_sujeto == "PERSONA_NATURAL":
            nombres = f"{random.choice(NOMBRES_BASE)} {random.choice(NOMBRES_BASE)}"
            apellidos = f"{random.choice(APELLIDOS_BASE)} {random.choice(APELLIDOS_BASE)}"
            fecha_nacimiento = _random_fecha_nacimiento()
        else:
            # ✅ Aumentamos diversidad para evitar miles de repetidos tipo "GOMEZ LTDA"
            nombres = f"{random.choice(APELLIDOS_BASE)} {random.choice(APELLIDOS_BASE)} {random.choice(TAGS_EMP)} {random.choice(SUFIJOS_EMP)} {random.randint(10, 9999)}"
            apellidos = None
            fecha_nacimiento = None

        key = _name_key(tipo_sujeto, nombres, apellidos)
        if key in seen_name_keys:
            continue  # regenerar

        seen_name_keys.add(key)

        nacionalidad = _pick_nacionalidad() if tipo_sujeto == "PERSONA_NATURAL" else None
        numero_documento, tipo_documento = _random_doc(tipo_sujeto)
        pais_residencia = _pick_pais_residencia()

        rows.append({
            "id_tercero": f"T{start_idx + i:07d}",
            "tipo_sujeto": tipo_sujeto,
            "nombres": nombres,
            "apellidos": apellidos,
            "fecha_nacimiento": fecha_nacimiento,
            "nacionalidad": nacionalidad,
            "numero_documento": numero_documento,
            "tipo_documento": tipo_documento,
            "pais_residencia": pais_residencia,
            "coincidencia": 0,
        })
        i += 1

    if i < n:
        raise RuntimeError(f"No se pudieron generar {n} ficticios únicos. Generados={i}. Amplía vocabulario base.")

    return rows

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_FILE)

    synthetic: List[Dict[str, Any]] = []
    next_id = 0
    seen_name_keys = set()

    # 9200 ficticios
    synthetic.extend(_gen_ficticios(N_FICTICIOS, next_id, seen_name_keys))
    next_id += N_FICTICIOS

    # 160 exactos (copiados de consolidado)
    exact_rows = _fetch_random_rows(conn, N_PER_CASE)
    for i, r in enumerate(exact_rows):
        synthetic.append(_row_to_synthetic_schema(r, f"T{next_id + i:07d}"))
    next_id += len(exact_rows)

    # 160 typo (consulto y edito)
    typo_rows = _fetch_random_rows(conn, N_PER_CASE)
    for i, r in enumerate(typo_rows):
        t = _row_to_synthetic_schema(r, f"T{next_id + i:07d}")
        # modificar nombres o apellidos
        if random.random() < 0.7:
            t["nombres"] = _one_char_typo(t.get("nombres"))
        else:
            t["apellidos"] = _one_char_typo(t.get("apellidos"))
        synthetic.append(t)
    next_id += len(typo_rows)

    # 160 partial (consulto y recorto)
    partial_rows = _fetch_random_rows(conn, N_PER_CASE)
    for i, r in enumerate(partial_rows):
        t = _row_to_synthetic_schema(r, f"T{next_id + i:07d}")
        n2, a2 = _partial_name(t.get("nombres"), t.get("apellidos"))
        t["nombres"] = n2
        t["apellidos"] = a2
        synthetic.append(t)
    next_id += len(partial_rows)

    # 160 alias (consulto aliases y uso un alias como "nombres")
    alias_rows = _fetch_rows_with_alias(conn, N_PER_CASE)
    for i, r in enumerate(alias_rows):
        t = _row_to_synthetic_schema(r, f"T{next_id + i:07d}")
        aliases = _json_load_list(r.get("aliases"))
        # tomamos un alias real
        alias_name = random.choice(aliases) if aliases else None
        # en tu esquema no existe "aliases", así que lo representamos como el nombre principal del tercero
        t["nombres"] = alias_name or t.get("nombres")
        # apellidos se mantiene o se puede dejar None (depende del alias)
        synthetic.append(t)
    next_id += len(alias_rows)

    # 160 doc (solo si hay numero_documento) + debilitamos nombre para forzar doc
    doc_rows = _fetch_rows_with_doc(conn, N_PER_CASE)
    for i, r in enumerate(doc_rows):
        t = _row_to_synthetic_schema(r, f"T{next_id + i:07d}")
        # hacemos nombre parcial para que el doc sea el feature fuerte
        n2, a2 = _partial_name(t.get("nombres"), t.get("apellidos"))
        t["nombres"] = n2
        t["apellidos"] = a2
        synthetic.append(t)
    next_id += len(doc_rows)

    conn.close()

    # DataFrame final con el esquema exacto
    df = pd.DataFrame(synthetic, columns=[
    "id_tercero",
    "tipo_sujeto",
    "nombres",
    "apellidos",
    "fecha_nacimiento",
    "nacionalidad",
    "numero_documento",
    "tipo_documento",
    "pais_residencia",
    "coincidencia",
    ])

    # Guardado CSV
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"OK: generado {len(df)} registros en {OUT_CSV}")

    # Resumen rápido
    print(df["tipo_sujeto"].value_counts(dropna=False))
    print("Docs nulos:", df["numero_documento"].isna().sum())


if __name__ == "__main__":
    main()