""" pipeline/normalizacion/paco_fgn_transform.py

Transformación PACO FGN CSV -> DataFrame canónico.

- Lee CSV en dtype=str para evitar inferencia
- Normaliza texto relevante
- Construye tipo_sancion combinando campos (Título/Capítulo/Artículo/...).
- Calcula hash_contenido e id_registro
- Retorna df (sin meta)

"""

import time
from pathlib import Path
import pandas as pd

from pipeline.utils import norm_text, stable_hash_from_dict, make_id_registro


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


def year_to_date(year_val):
    try:
        y = int(year_val)
        if 1900 <= y <= 2100:
            return f"{y}-01-01"
    except Exception:
        pass
    return None


def transform_paco_fgn(raw_csv_path: Path, fecha_ingesta_iso: str | None = None) -> pd.DataFrame:
    """
    PACO FGN (sanciones_penales_FGN.csv) -> DataFrame CANÓNICO uniforme.

    Salida:
      DataFrame con las columnas CANON_COLS (mismo esquema que todas las fuentes).
    """
    if fecha_ingesta_iso is None:
        fecha_ingesta_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. Leer CSV
    df_raw = pd.read_csv(raw_csv_path, encoding="utf-8", dtype=str)

    # 2. Normalizar columnas de texto relevantes (si existen)
    for col in ["DEPARTAMENTO", "mpio", "TITULO", "CAPITULO", "ARTICULO"]:
        if col in df_raw.columns:
            df_raw[col] = df_raw[col].map(norm_text)

    # 3. Derivar fecha_sancion desde AÑO_ACTUACION si existe
    if "AÑO_ACTUACION" in df_raw.columns:
        fecha_sancion = df_raw["AÑO_ACTUACION"].map(year_to_date)
    else:
        fecha_sancion = pd.Series([None] * len(df_raw))

    # 4. origen_id (id de la fuente)
    if "id" in df_raw.columns:
        origen_id = (
            df_raw["id"]
            .astype("string")
            .str.strip()
            .where(lambda s: s.notna() & (s != ""), None)
        )
    else:
        origen_id = pd.Series([None] * len(df_raw))

    # 5. nombres: usando ARTICULO como lo hacías
    if "ARTICULO" in df_raw.columns:
        nombres = df_raw["ARTICULO"].map(lambda x: norm_text(x) if x else None)
    else:
        nombres = pd.Series([None] * len(df_raw))

    # 6. tipo_sancion (por fila, pero sin iterrows)
    def build_tipo_sancion_row(r: pd.Series) -> str | None:
        parts = []
        tit = r.get("TITULO")
        cap = r.get("CAPITULO")
        art = r.get("ARTICULO")
        dep = r.get("DEPARTAMENTO")
        mpi = r.get("mpio")
        dane = r.get("CODIGO_DANE_MUNICIPIO")

        if tit: parts.append(f"Titulo={tit}")
        if cap: parts.append(f"Capitulo={cap}")
        if art: parts.append(f"Articulo={art}")
        if dep: parts.append(f"Depto={dep}")
        if mpi: parts.append(f"Mpio={mpi}")

        if dane and str(dane).strip():
            try:
                parts.append(f"DANE={int(float(dane))}")
            except Exception:
                parts.append(f"DANE={str(dane).strip()}")

        return " | ".join(parts) if parts else None

    tipo_sancion = df_raw.apply(build_tipo_sancion_row, axis=1)

    # 7. Construcción DF canónico (sin hash e id aún)
    df = pd.DataFrame({
        "id_registro": None,
        "fuente": "PACO_FGN",
        "tipo_sujeto": "PERSONA_JURIDICA",
        "nombres": nombres,
        "apellidos": None,
        "aliases": [[] for _ in range(len(df_raw))],
        "fecha_nacimiento": None,
        "nacionalidad": [[] for _ in range(len(df_raw))],
        "numero_documento": None,
        "tipo_sancion": tipo_sancion,
        "fecha_sancion": fecha_sancion,
        "fecha_vencimiento": None,
        "activo": True,
        "fecha_ingesta": fecha_ingesta_iso,
        "origen_id": origen_id,
        "hash_contenido": None,
    })

    # 8. hash_contenido estable (excluimos fecha_ingesta)
    def row_hash(row: pd.Series) -> str:
        d = row.to_dict()
        d.pop("fecha_ingesta", None)
        d.pop("id_registro", None)
        d.pop("hash_contenido", None)
        return stable_hash_from_dict(d)

    df["hash_contenido"] = df.apply(row_hash, axis=1)

    # 9. id_registro estable
    df["id_registro"] = df["hash_contenido"].map(lambda h: make_id_registro("PACO_FGN", h))

    # 10. Garantizar orden y columnas finales
    df = df[CANON_COLS]

    return df