import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from typing import Dict, Iterator, Any, List, Tuple

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


def _strip_ns(tag: str) -> str:
    """Remueve namespace de un tag XML: {ns}tag -> tag"""
    return tag.split("}", 1)[1] if "}" in tag else tag


def _parse_ofac_xml_to_rows(raw_xml_path: Path) -> Iterator[Dict[str, Any]]:
    """
    Parser streaming del XML OFAC SDN.

    Devuelve "rows" intermedios (NO canónicos) con:
      - entity_type, identity_id
      - primary_full, primary_last
      - aliases (list)
      - date_published
      - lists, programs, sanction_types (list)
    """
    ctx = ET.iterparse(str(raw_xml_path), events=("start", "end"))
    _, root = next(ctx)

    # namespace dinámico
    ns_uri = root.tag.split("}")[0].strip("{") if "}" in root.tag else ""
    ns = {"ofac": ns_uri} if ns_uri else {}

    # refId -> normalized text value
    ref_dict: Dict[str, str] = {}

    for event, elem in ctx:
        tag = _strip_ns(elem.tag)

        # A) referenceValue(refId) -> value
        if event == "end" and tag == "referenceValue":
            ref_id = elem.attrib.get("refId")
            val = elem.findtext("ofac:value", default=None, namespaces=ns) if ns else elem.findtext("value")
            val = norm_text(val)
            if ref_id and val:
                ref_dict[ref_id] = val
            elem.clear()
            continue

        # B) entity -> construir row
        if event == "end" and tag == "entity":

            # entity_type, identity_id
            if ns:
                entity_type = elem.findtext("ofac:generalInfo/ofac:entityType", default=None, namespaces=ns)
                identity_id = elem.findtext("ofac:generalInfo/ofac:identityId", default=None, namespaces=ns)
            else:
                entity_type = elem.findtext("generalInfo/entityType")
                identity_id = elem.findtext("generalInfo/identityId")

            entity_type = norm_text(entity_type)
            identity_id = norm_text(identity_id)

            # primary + aliases
            primary_full = None
            primary_last = None
            aliases: List[str] = []

            names = elem.find("ofac:names", ns) if ns else elem.find("names")
            if names is not None:
                name_nodes = names.findall("ofac:name", ns) if ns else names.findall("name")
                for name in name_nodes:
                    is_primary_txt = (
                        name.findtext("ofac:isPrimary", default="false", namespaces=ns) if ns
                        else name.findtext("isPrimary", default="false")
                    )
                    is_primary = (is_primary_txt or "").strip().lower() == "true"

                    tr = name.find("ofac:translations/ofac:translation", ns) if ns else name.find("translations/translation")
                    if tr is None:
                        continue

                    full = tr.findtext("ofac:formattedFullName", default=None, namespaces=ns) if ns else tr.findtext("formattedFullName")
                    last = tr.findtext("ofac:formattedLastName", default=None, namespaces=ns) if ns else tr.findtext("formattedLastName")

                    full = norm_text(full)
                    last = norm_text(last)

                    if not full:
                        continue

                    if is_primary and primary_full is None:
                        primary_full = full
                        primary_last = last
                    else:
                        aliases.append(full)

            # datePublished (del primer sanctionsList si existe)
            sl = elem.find("ofac:sanctionsLists/ofac:sanctionsList", ns) if ns else elem.find("sanctionsLists/sanctionsList")
            date_pub = sl.attrib.get("datePublished") if sl is not None else None
            date_pub = norm_text(date_pub)

            # helper: resolver refIds
            def ref_vals(path: str, node: str) -> List[str]:
                out: List[str] = []
                parent = elem.find(path, ns) if ns else elem.find(path.replace("ofac:", "").replace("/", "/"))
                if parent is None:
                    return out
                children = parent.findall(f"ofac:{node}", ns) if ns else parent.findall(node)
                for n in children:
                    rid = n.attrib.get("refId")
                    if rid and rid in ref_dict:
                        out.append(ref_dict[rid])
                return list(dict.fromkeys(out))  # dedupe manteniendo orden

            lists = ref_vals("ofac:sanctionsLists", "sanctionsList") if ns else ref_vals("sanctionsLists", "sanctionsList")
            programs = ref_vals("ofac:sanctionsPrograms", "sanctionsProgram") if ns else ref_vals("sanctionsPrograms", "sanctionsProgram")
            stypes = ref_vals("ofac:sanctionsTypes", "sanctionsType") if ns else ref_vals("sanctionsTypes", "sanctionsType")

            yield {
                "entity_type": entity_type,
                "identity_id": identity_id,
                "primary_full": primary_full,
                "primary_last": primary_last,
                "aliases": list(dict.fromkeys(aliases)),  # dedupe
                "date_published": date_pub,
                "lists": lists,
                "programs": programs,
                "sanction_types": stypes,
            }

            elem.clear()


def transform_ofac_sdn(raw_xml_path: Path, fecha_ingesta_iso: str | None = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    OFAC SDN XML -> (DataFrame CANÓNICO uniforme, meta)

    Antes devolvía un iterator streaming; ahora (Opción A) materializamos a DataFrame,
    porque tu volumen total (~70k) lo permite y simplifica QC.
    """
    if fecha_ingesta_iso is None:
        fecha_ingesta_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    # 1) XML -> rows (dicts intermedios) -> lista (materializamos)
    rows = list(_parse_ofac_xml_to_rows(raw_xml_path))
    if not rows:
        raise ValueError("No se pudieron extraer entidades válidas desde OFAC (0 rows).")

    df_rows = pd.DataFrame(rows)

    # 2) Determinar tipo_sujeto según entity_type
    entity_type = df_rows["entity_type"].fillna("").astype(str).str.strip().str.lower()
    tipo_sujeto = entity_type.map(lambda x: "PERSONA_NATURAL" if x == "individual" else "PERSONA_JURIDICA")

    # 3) nombres y apellidos
    nombres = df_rows["primary_full"].map(norm_text) if "primary_full" in df_rows.columns else pd.Series([None] * len(df_rows))
    primary_last = df_rows["primary_last"].map(norm_text) if "primary_last" in df_rows.columns else pd.Series([None] * len(df_rows))

    # Solo persona natural lleva apellidos (como en tu lógica original)
    apellidos = primary_last.where(tipo_sujeto == "PERSONA_NATURAL", None)

    # 4) tipo_sancion a partir de lists/programs/sanction_types
    def build_tipo_sancion_row(r: pd.Series) -> str | None:
        lists = r.get("lists") if isinstance(r.get("lists"), list) else []
        programs = r.get("programs") if isinstance(r.get("programs"), list) else []
        stypes = r.get("sanction_types") if isinstance(r.get("sanction_types"), list) else []

        parts = []
        if lists:
            parts.append(f"List={', '.join(lists)}")
        if programs:
            parts.append(f"Program={', '.join(programs)}")
        if stypes:
            parts.append(f"Type={', '.join(stypes)}")

        return " | ".join(parts) if parts else None

    tipo_sancion = df_rows.apply(build_tipo_sancion_row, axis=1)

    # 5) fecha_sancion (date_published parseado)
    fecha_sancion = df_rows["date_published"].map(parse_date_iso) if "date_published" in df_rows.columns else pd.Series([None] * len(df_rows))

    # 6) aliases y origen_id
    # aliases: aseguramos lista siempre
    def safe_list(x):
        return x if isinstance(x, list) else []

    aliases = df_rows["aliases"].map(safe_list) if "aliases" in df_rows.columns else pd.Series([[] for _ in range(len(df_rows))])
    origen_id = df_rows["identity_id"].map(norm_text) if "identity_id" in df_rows.columns else pd.Series([None] * len(df_rows))

    # 7) Armar DF canónico (sin hash todavía)
    df = pd.DataFrame({
        "id_registro": None,
        "fuente": "OFAC",
        "tipo_sujeto": tipo_sujeto,
        "nombres": nombres,
        "apellidos": apellidos,
        "aliases": aliases,
        "fecha_nacimiento": None,
        "nacionalidad": [[] for _ in range(len(df_rows))],
        "numero_documento": None,
        "tipo_sancion": tipo_sancion,
        "fecha_sancion": fecha_sancion,
        "fecha_vencimiento": None,
        "activo": True,
        "fecha_ingesta": fecha_ingesta_iso,
        "origen_id": origen_id,
        "hash_contenido": None,
    })

    # 8) Hash estable (sin fecha_ingesta, id_registro, hash_contenido)
    def row_hash(row: pd.Series) -> str:
        d = row.to_dict()
        d.pop("fecha_ingesta", None)
        d.pop("id_registro", None)
        d.pop("hash_contenido", None)
        return stable_hash_from_dict(d)

    df["hash_contenido"] = df.apply(row_hash, axis=1)
    df["id_registro"] = df["hash_contenido"].map(lambda h: make_id_registro("OFAC", h))

    # 9) Orden final
    df = df[CANON_COLS]

    meta = {
        "fecha_ingesta": fecha_ingesta_iso,
        "mode": "dataframe",
        "source": "OFAC",
        "entities_parsed": len(df_rows),
        "records_out": len(df),
    }

    return df, meta