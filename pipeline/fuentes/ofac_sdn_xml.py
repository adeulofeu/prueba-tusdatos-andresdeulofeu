""" pipeline/fuentes/ofac_sdn_xml.py

Extractor OFAC SDN (XML enhanced).

- Descarga el XML a data/raw/ofac_sdn/YYYYMMDD/SDN_ENHANCED.xml
- Retorna (by_source, durations, errors)

"""

import os
import time
from pathlib import Path

from pipeline.utils import HTTPClient


def download_raw_xml(client: HTTPClient, url: str, out_path: Path, logger):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"[OFAC_SDN] Downloading XML from: {url}")

    resp = client.get(url, stream=True)

    # Guardado streaming (evita tener 100MB en memoria)
    with out_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):  # 1MB
            if chunk:
                f.write(chunk)

    if out_path.stat().st_size == 0:
        raise ValueError("OFAC_SDN: archivo vacío o problemas con el servidor de datos")

    logger.info(f"[OFAC_SDN] Saved XML: {out_path} ({out_path.stat().st_size} bytes)")


def extract_ofac_sdn(data_dir: Path, logger):
    """
    Retorna: by_source (dict), durations (dict), errors (list)
    """
    url = os.getenv("OFAC_SDN_XML_URL", "").strip()
    durations = {}
    errors = []

    if not url:
        errors.append({"stage": "extract", "error": "OFAC_SDN_XML_URL no está configurada en .env"})
        return {"status": "SKIPPED"}, durations, errors

    yyyymmdd = time.strftime("%Y%m%d")
    raw_dir = data_dir / "raw" / "ofac_sdn" / yyyymmdd
    xml_path = raw_dir / "SDN_ENHANCED.xml"

    # Se crea el cliente HTTPS a traves de la clase auxiliar en utils.py
    client = HTTPClient(logger)

    try:
        t_dl = time.time()
        download_raw_xml(client, url, xml_path, logger)
        durations["download_sec"] = round(time.time() - t_dl, 4)

        by_source = {
            "status": "OK",
            "raw_xml_path": str(xml_path),
            "raw_xml_bytes": xml_path.stat().st_size,
        }
        return by_source, durations, errors

    except Exception as e:
        msg = str(e)
        logger.error(f"[OFAC_SDN] Extract failed: {msg}")
        errors.append({"stage": "extract", "error": msg})
        return {"status": "FAILED"}, durations, errors