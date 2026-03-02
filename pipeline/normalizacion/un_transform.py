import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Tuple

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


def join_name(parts):
    parts = [norm_text(p) for p in parts]
    parts = [p for p in parts if p]
    return " ".join(parts) if parts else None


def parse_dob(ind_elem):
    dob = ind_elem.find("INDIVIDUAL_DATE_OF_BIRTH")
    if dob is None:
        return None

    date_txt = norm_text(dob.findtext("DATE"))
    if date_txt:
        return date_txt

    year_txt = norm_text(dob.findtext("YEAR"))
    if year_txt and year_txt.isdigit():
        return f"{year_txt}-01-01"

    return None


def collect_nationalities(elem):
    vals = []
    for n in elem.findall("NATIONALITY"):
        v = norm_text(n.findtext("VALUE"))
        if v:
            vals.append(v)
    return list(dict.fromkeys(vals))


def collect_aliases(elem, alias_tag):
    aliases = []
    for a in elem.findall(alias_tag):
        v = norm_text(a.findtext("ALIAS_NAME"))
        if v:
            aliases.append(v)
    return list(dict.fromkeys(aliases))


def tipo_sancion(elem):
    un_list_type = norm_text(elem.findtext("UN_LIST_TYPE"))
    ref = norm_text(elem.findtext("REFERENCE_NUMBER"))

    list_type = None
    lt = elem.find("LIST_TYPE")
    if lt is not None:
        list_type = norm_text(lt.findtext("VALUE"))

    parts = []
    if list_type:
        parts.append(f"ListType={list_type}")
    if un_list_type:
        parts.append(f"UNList={un_list_type}")
    if ref:
        parts.append(f"Ref={ref}")

    return " | ".join(parts) if parts else None


def first_document_number(ind_elem):
    doc = ind_elem.find("INDIVIDUAL_DOCUMENT")
    if doc is None:
        return None
    return norm_text(doc.findtext("NUMBER"))


def transform_un_consolidated(raw_xml_path: Path, fecha_ingesta_iso: str | None = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    UN consolidated.xml -> (DataFrame CANÓNICO uniforme, meta)

    Antes devolvía generator streaming; ahora (Opción A) materializamos a DataFrame
    porque simplifica QC y tu volumen total (~70k) lo permite.
    """
    if fecha_ingesta_iso is None:
        fecha_ingesta_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    # 1) Parse XML y construir lista de dicts canónicos (sin hash aún)
    canon_rows: List[Dict[str, Any]] = []

    ctx = ET.iterparse(str(raw_xml_path), events=("end",))
    for _, elem in ctx:
        tag = elem.tag

        if tag == "INDIVIDUAL":
            dataid = norm_text(elem.findtext("DATAID"))
            listed_on = norm_text(elem.findtext("LISTED_ON"))

            nombre = join_name([
                elem.findtext("FIRST_NAME"),
                elem.findtext("SECOND_NAME"),
                elem.findtext("THIRD_NAME"),
                elem.findtext("FOURTH_NAME"),
            ])

            canon_rows.append({
                "id_registro": None,
                "fuente": "UN",
                "tipo_sujeto": "PERSONA_NATURAL",
                "nombres": nombre,
                "apellidos": None,
                "aliases": collect_aliases(elem, "INDIVIDUAL_ALIAS"),
                "fecha_nacimiento": parse_dob(elem),
                "nacionalidad": collect_nationalities(elem),
                "numero_documento": first_document_number(elem),
                "tipo_sancion": tipo_sancion(elem),
                "fecha_sancion": listed_on,
                "fecha_vencimiento": None,
                "activo": True,
                "fecha_ingesta": fecha_ingesta_iso,
                "origen_id": dataid,
                "hash_contenido": None,
            })

            elem.clear()

        elif tag == "ENTITY":
            dataid = norm_text(elem.findtext("DATAID"))
            listed_on = norm_text(elem.findtext("LISTED_ON"))

            nombre = join_name([
                elem.findtext("FIRST_NAME"),
                elem.findtext("SECOND_NAME"),
                elem.findtext("THIRD_NAME"),
                elem.findtext("FOURTH_NAME"),
            ])

            canon_rows.append({
                "id_registro": None,
                "fuente": "UN",
                "tipo_sujeto": "PERSONA_JURIDICA",
                "nombres": nombre,
                "apellidos": None,
                "aliases": collect_aliases(elem, "ENTITY_ALIAS"),
                "fecha_nacimiento": None,
                "nacionalidad": [],
                "numero_documento": None,
                "tipo_sancion": tipo_sancion(elem),
                "fecha_sancion": listed_on,
                "fecha_vencimiento": None,
                "activo": True,
                "fecha_ingesta": fecha_ingesta_iso,
                "origen_id": dataid,
                "hash_contenido": None,
            })

            elem.clear()

    if not canon_rows:
        raise ValueError("No se pudieron extraer registros válidos desde UN (0 rows).")

    # 2) Lista -> DataFrame
    df = pd.DataFrame(canon_rows)

    # 3) Hash estable (sin fecha_ingesta, id_registro, hash_contenido)
    def row_hash(row: pd.Series) -> str:
        d = row.to_dict()
        d.pop("fecha_ingesta", None)
        d.pop("id_registro", None)
        d.pop("hash_contenido", None)
        return stable_hash_from_dict(d)

    df["hash_contenido"] = df.apply(row_hash, axis=1)
    df["id_registro"] = df["hash_contenido"].map(lambda h: make_id_registro("UN", h))

    # 4) Orden final uniforme
    df = df[CANON_COLS]

    meta = {
        "fecha_ingesta": fecha_ingesta_iso,
        "mode": "dataframe",
        "source": "UN",
        "records_out": len(df),
    }

    return df, meta