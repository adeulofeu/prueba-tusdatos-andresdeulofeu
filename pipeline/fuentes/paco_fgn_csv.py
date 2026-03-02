import os
import time
from pathlib import Path

from pipeline.utils import HTTPClient


def download_csv(client: HTTPClient, url: str, out_path: Path, logger):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"[PACO_FGN] Downloading CSV from: {url}")

    resp = client.get(url, stream=True)

    # Streaming para consistencia
    with out_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    if out_path.stat().st_size == 0:
        raise ValueError("PACO_FGN: archivo vacío o problemas con el servidor de datos")

    logger.info(f"[PACO_FGN] Saved CSV: {out_path} ({out_path.stat().st_size} bytes)")


def extract_paco_fgn(data_dir: Path, logger):
    """
    Retorna: by_source (dict), durations (dict), errors (list)
    """
    url = os.getenv("PACO_FGN_CSV_URL", "").strip()
    durations = {}
    errors = []

    if not url:
        errors.append({"stage": "extract", "error": "PACO_FGN_CSV_URL no está configurada en .env"})
        return {"status": "SKIPPED"}, durations, errors

    yyyymmdd = time.strftime("%Y%m%d")
    raw_dir = data_dir / "raw" / "paco_fgn" / yyyymmdd
    csv_path = raw_dir / "sanciones_penales_FGN.csv"

    client = HTTPClient(logger)

    try:
        t_dl = time.time()
        download_csv(client, url, csv_path, logger)
        durations["download_sec"] = round(time.time() - t_dl, 4)

        by_source = {
            "status": "OK",
            "raw_csv_path": str(csv_path),
            "raw_csv_bytes": csv_path.stat().st_size,
        }
        return by_source, durations, errors

    except Exception as e:
        msg = str(e)
        logger.error(f"[PACO_FGN] Extract failed: {msg}")
        errors.append({"stage": "extract", "error": msg})
        return {"status": "FAILED"}, durations, errors