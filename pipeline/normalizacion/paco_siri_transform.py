""" pipeline/normalizacion/paco_siri_transform.py

Transformación PACO SIRI TXT -> DataFrame canónico.

- Lectura robusta del TXT: prueba utf-8 y latin-1
- Descarta filas malformadas (número de columnas != esperado)
- Normaliza texto y parsea fecha_acto a fecha_sancion (ISO)
- Calcula hash_contenido e id_registro
- Retorna (df, meta)

"""
import time
import csv
from pathlib import Path
import pandas as pd

from pipeline.utils import norm_text, parse_date_iso, stable_hash_from_dict, make_id_registro


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


COLS = [
    "id_fuente", "tipo_proceso", "tipo_sujeto_raw", "flag_1",
    "tipo_doc", "num_doc",
    "apellido_1", "apellido_2", "nombre_1", "nombre_2",
    "cargo", "departamento", "municipio",
    "sancion",
    "duracion_1", "duracion_2", "duracion_3",
    "instancia",
    "autoridad",
    "fecha_acto",
    "radicado",
    "entidad",
    "depto_entidad",
    "mpio_entidad",
    "anio", "mes", "dia",
    "duracion_texto",
]


def safe_join(parts):
    """
    Une varias piezas de texto en un solo string:
    1. Normaliza cada parte con norm_text
    2. Filtra vacíos
    3. Une con espacios
    """
    parts = [norm_text(p) for p in parts]
    parts = [p for p in parts if p]
    return " ".join(parts) if parts else None


def _read_paco_siri_robust(path: Path, expected_cols: int):
    """
    Lector tolerante:
    - intenta utf-8 y luego latin-1
    - usa csv.reader (respeta comillas)
    - descarta filas con columnas != expected_cols
    Retorna: (rows, encoding_used, bad_rows_count)
    """
    last_err = None
    for enc in ["utf-8", "latin-1"]:
        try:
            rows = []
            bad_rows = 0
            with path.open("r", encoding=enc, errors="replace", newline="") as f:
                reader = csv.reader(f, delimiter=",", quotechar='"')
                for row in reader:
                    if not row:
                        continue
                    if len(row) != expected_cols:
                        bad_rows += 1
                        continue
                    rows.append(row)
            return rows, enc, bad_rows
        except Exception as e:
            last_err = e
    raise last_err


def transform_paco_siri(raw_txt_path: Path, fecha_ingesta_iso: str | None = None):
    """
    PACO SIRI TXT -> (DataFrame CANÓNICO uniforme, meta)

    Salida:
      df_canon: DataFrame con columnas CANON_COLS (uniforme para todas las fuentes)
      meta: {encoding_used, bad_rows_skipped, rows_parsed, records_out}
    """
    if fecha_ingesta_iso is None:
        fecha_ingesta_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. Leer el TXT de forma robusta (encoding + filas malformadas)
    rows, enc_used, bad_rows = _read_paco_siri_robust(raw_txt_path, expected_cols=len(COLS))
    if not rows:
        raise ValueError("No se pudieron leer registros válidos (0 filas parseadas).")

    # 2. Rows -> DataFrame raw
    df_raw = pd.DataFrame(rows, columns=COLS)

    # 3. Normalizar columnas de texto relevantes
    for c in [
        "tipo_doc", "apellido_1", "apellido_2", "nombre_1", "nombre_2", "cargo",
        "departamento", "municipio", "sancion", "instancia", "autoridad", "entidad",
        "tipo_proceso", "id_fuente", "num_doc"
    ]:
        if c in df_raw.columns:
            df_raw[c] = df_raw[c].map(norm_text)

    # 4. Derivar fecha_sancion desde fecha_acto
    df_raw["fecha_sancion"] = df_raw["fecha_acto"].map(parse_date_iso)

    # 5. Construcción canónica en DataFrame (sin iterrows)
    #    - nombres: nombre_1 + nombre_2
    #    - apellidos: apellido_1 + apellido_2
    nombres = df_raw.apply(lambda r: safe_join([r.get("nombre_1"), r.get("nombre_2")]), axis=1)
    apellidos = df_raw.apply(lambda r: safe_join([r.get("apellido_1"), r.get("apellido_2")]), axis=1)

    # tipo_sancion: compone string con sancion + tipo_proceso + instancia + entidad
    def build_tipo_sancion_row(r: pd.Series) -> str | None:
        parts = []

        sanc = r.get("sancion")
        if sanc:
            parts.append(sanc)

        tp = r.get("tipo_proceso")
        if tp:
            parts.append(str(tp))

        inst = r.get("instancia")
        if inst:
            parts.append(f"Instancia={inst}")

        ent = r.get("entidad")
        if ent:
            parts.append(f"Entidad={ent}")

        return " | ".join([p for p in parts if p]) or None

    tipo_sancion = df_raw.apply(build_tipo_sancion_row, axis=1)

    # documento + origen_id
    numero_documento = df_raw["num_doc"].map(norm_text) if "num_doc" in df_raw.columns else pd.Series([None] * len(df_raw))
    origen_id = df_raw["id_fuente"].map(norm_text) if "id_fuente" in df_raw.columns else pd.Series([None] * len(df_raw))

    # 6. Armar DF canónico (sin hash aún)
    df = pd.DataFrame({
        "id_registro": None,
        "fuente": "PACO_SIRI",
        "tipo_sujeto": "PERSONA_NATURAL",
        "nombres": nombres,
        "apellidos": apellidos,
        "aliases": [[] for _ in range(len(df_raw))],
        "fecha_nacimiento": None,
        "nacionalidad": [[] for _ in range(len(df_raw))],
        "numero_documento": numero_documento,
        "tipo_sancion": tipo_sancion,
        "fecha_sancion": df_raw["fecha_sancion"],
        "fecha_vencimiento": None,
        "activo": True,
        "fecha_ingesta": fecha_ingesta_iso,
        "origen_id": origen_id,
        "hash_contenido": None,
    })

    # 7. Hash estable (sin fecha_ingesta, id_registro, hash_contenido)
    def row_hash(row: pd.Series) -> str:
        d = row.to_dict()
        d.pop("fecha_ingesta", None)
        d.pop("id_registro", None)
        d.pop("hash_contenido", None)
        return stable_hash_from_dict(d)

    df["hash_contenido"] = df.apply(row_hash, axis=1)
    df["id_registro"] = df["hash_contenido"].map(lambda h: make_id_registro("PACO_SIRI", h))

    # 8. Garantizar columnas finales y orden
    df = df[CANON_COLS]

    # 9. Meta de auditoría
    meta = {
        "encoding_used": enc_used,
        "bad_rows_skipped": bad_rows,
        "rows_parsed": len(rows),
        "records_out": len(df),
        "fecha_ingesta": fecha_ingesta_iso,
    }

    return df, meta