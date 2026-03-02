import os
import time
import zipfile
from pathlib import Path

from pipeline.utils import HTTPClient


def download_raw_zip(client: HTTPClient, url: str, out_path: Path, logger):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"[PACO_SIRI] Downloading RAW ZIP from: {url}")

    resp = client.get(url, stream=True)

    with out_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    if out_path.stat().st_size == 0:
        raise ValueError("PACO_SIRI: ZIP vacío o problemas con el servidor de datos")

    logger.info(f"[PACO_SIRI] Saved RAW ZIP: {out_path} ({out_path.stat().st_size} bytes)")


def extract_txt_from_zip(zip_path: Path, extract_dir: Path, logger) -> Path:
    extract_dir.mkdir(parents=True, exist_ok=True)

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            members = [n for n in zf.namelist() if not n.endswith("/")]

            if not members:
                raise ValueError("PACO_SIRI: ZIP no contiene archivos")

            # Preferimos .txt; si no hay, tomamos el primer archivo
            txt_candidates = [n for n in members if n.lower().endswith(".txt")]
            chosen = txt_candidates[0] if txt_candidates else members[0]

            logger.info(f"[PACO_SIRI] Extracting file from ZIP: {chosen}")

            extracted_path = extract_dir / Path(chosen).name

            # Copia streaming (evita leer todo a RAM)
            with zf.open(chosen) as src, extracted_path.open("wb") as dst:
                while True:
                    buf = src.read(1024 * 1024)
                    if not buf:
                        break
                    dst.write(buf)

    except zipfile.BadZipFile:
        raise ValueError("PACO_SIRI: ZIP corrupto o inválido (BadZipFile)")

    if not extracted_path.exists() or extracted_path.stat().st_size == 0:
        raise ValueError("PACO_SIRI: archivo extraído está vacío")

    return extracted_path


def extract_paco_siri(data_dir: Path, logger):
    """
    Retorna: by_source (dict), durations (dict), errors (list)
    """
    url = os.getenv("PACO_SIRI_ZIP_URL", "").strip()
    durations = {}
    errors = []

    if not url:
        errors.append({"stage": "extract", "error": "PACO_SIRI_ZIP_URL no está configurada en .env"})
        return {"status": "SKIPPED"}, durations, errors

    yyyymmdd = time.strftime("%Y%m%d")
    raw_dir = data_dir / "raw" / "paco_siri" / yyyymmdd
    zip_path = raw_dir / "antecedentes_SIRI_sanciones_Cleaned.zip"
    extract_dir = raw_dir / "extracted"

    client = HTTPClient(logger)

    try:
        # Download ZIP
        t_dl = time.time()
        download_raw_zip(client, url, zip_path, logger)
        durations["download_sec"] = round(time.time() - t_dl, 4)

        # Extract TXT (o primer archivo)
        t_ext = time.time()
        extracted_path = extract_txt_from_zip(zip_path, extract_dir, logger)
        durations["extract_sec"] = round(time.time() - t_ext, 4)

        by_source = {
            "status": "OK",
            "raw_zip_path": str(zip_path),
            "raw_zip_bytes": zip_path.stat().st_size,
            "extracted_file_path": str(extracted_path),
            "extracted_file_name": extracted_path.name,
            "extracted_file_bytes": extracted_path.stat().st_size,
        }
        return by_source, durations, errors

    except Exception as e:
        msg = str(e)
        logger.error(f"[PACO_SIRI] Extract failed: {msg}")
        errors.append({"stage": "extract", "error": msg})
        return {"status": "FAILED"}, durations, errors