import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path

from pipeline.utils import HTTPClient


def _download_raw(client: HTTPClient, url: str, out_path: Path, logger):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"[EU] Downloading RAW from: {url}")

    # Nota: para EU puede ser importante enviar User-Agent (ya lo hace tu HTTPClient)
    resp = client.get(url, stream=True)

    # Guardado streaming (mejor para archivos grandes)
    with out_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    if out_path.stat().st_size == 0:
        raise ValueError("EU: archivo vacío (posible bloqueo/servidor)")

    logger.info(f"[EU] Saved RAW: {out_path} ({out_path.stat().st_size} bytes)")


def _parse_counts(xml_path: Path) -> dict:
    """
    Parse mínimo (conteos).
    La lista EU puede cambiar estructura. Contamos:
    - total de nodos
    - heurística de registros por tags comunes
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    total_nodes = sum(1 for _ in root.iter())

    candidate_tags = {"sanctionEntity", "entity", "individual", "person", "designation", "subject"}
    records = 0
    for el in root.iter():
        tag = el.tag
        if isinstance(tag, str) and "}" in tag:
            tag = tag.split("}", 1)[1]
        if isinstance(tag, str) and tag in candidate_tags:
            records += 1

    return {"total_nodes": total_nodes, "total_records_estimate": records}


def extract_eu(data_dir: Path, logger):
    """
    Retorna: by_source (dict), durations (dict), errors (list)
    """
    eu_url = os.getenv("EU_XML_URL", "").strip()
    durations = {}
    errors = []

    if not eu_url:
        errors.append({"stage": "extract", "error": "EU_XML_URL no está configurada en .env"})
        return {"status": "SKIPPED"}, durations, errors

    yyyymmdd = time.strftime("%Y%m%d")
    raw_path = data_dir / "raw" / "eu" / yyyymmdd / "eu_sanctions.xml"

    client = HTTPClient(logger)

    try:
        # Download
        t_dl = time.time()
        _download_raw(client, eu_url, raw_path, logger)
        durations["download_sec"] = round(time.time() - t_dl, 4)

        # Parse
        t_parse = time.time()
        counts = _parse_counts(raw_path)
        durations["parse_sec"] = round(time.time() - t_parse, 4)

        by_source = {
            "status": "OK",
            "raw_path": str(raw_path),
            "raw_bytes": raw_path.stat().st_size,
            **counts,
        }
        return by_source, durations, errors

    except Exception as e:
        msg = str(e)

        # Si EU da 403, no lo escondas: esto demuestra criterio
        logger.error(f"[EU] Extract failed: {msg}")
        errors.append({"stage": "extract", "error": msg})
        return {"status": "FAILED"}, durations, errors