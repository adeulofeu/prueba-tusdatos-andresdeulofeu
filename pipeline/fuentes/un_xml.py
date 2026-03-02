import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path

from pipeline.utils import HTTPClient


def download_raw(client: HTTPClient, url: str, out_path: Path, logger):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"[UN] Downloading RAW from: {url}")

    resp = client.get(url, stream=True)

    with out_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    if out_path.stat().st_size == 0:
        raise ValueError("UN: archivo vacío o problemas con el servidor de datos")

    logger.info(f"[UN] Saved RAW: {out_path} ({out_path.stat().st_size} bytes)")


def parse_counts(xml_path: Path) -> dict:
    tree = ET.parse(xml_path)
    root = tree.getroot()
    individuals = root.findall(".//INDIVIDUAL")
    entities = root.findall(".//ENTITY")
    return {
        "individuals": len(individuals),
        "entities": len(entities),
        "total_records": len(individuals) + len(entities),
    }


def extract_un(data_dir: Path, logger):
    un_url = os.getenv("UN_XML_URL", "").strip()
    durations = {}
    errors = []

    if not un_url:
        errors.append({"stage": "extract", "error": "UN_XML_URL no está configurada en .env"})
        return {"status": "SKIPPED"}, durations, errors

    yyyymmdd = time.strftime("%Y%m%d")
    raw_path = data_dir / "raw" / "un" / yyyymmdd / "consolidated.xml"

    client = HTTPClient(logger)

    try:
        # Download
        t_dl = time.time()
        download_raw(client, un_url, raw_path, logger)
        durations["download_sec"] = round(time.time() - t_dl, 4)

        # Parse
        t_parse = time.time()
        counts = parse_counts(raw_path)
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
        logger.error(f"[UN] Extract failed: {msg}")
        errors.append({"stage": "extract", "error": msg})
        return {"status": "FAILED"}, durations, errors