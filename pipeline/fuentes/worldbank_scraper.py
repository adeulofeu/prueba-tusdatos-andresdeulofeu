import os
import time
import hashlib
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup


def build_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # User-Agent ayuda a evitar bloqueos simples
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; tudatos-pipeline/1.0)"
    })
    return session


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def load_seen_ids(state_path: Path) -> set[str]:
    if not state_path.exists():
        return set()
    return set(state_path.read_text(encoding="utf-8").splitlines())


def append_seen_ids(state_path: Path, new_ids: list[str]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with state_path.open("a", encoding="utf-8") as f:
        for rid in new_ids:
            f.write(rid + "\n")


def fetch_html(session, url: str, timeout_sec: int) -> str:
    r = session.get(url, timeout=timeout_sec)
    r.raise_for_status()
    return r.text


def save_html(html_dir: Path, page_num: int, html: str) -> Path:
    html_dir.mkdir(parents=True, exist_ok=True)
    p = html_dir / f"page_{page_num:04d}.html"
    p.write_text(html, encoding="utf-8", errors="ignore")
    return p


def extract_rows_from_html(html: str) -> list[list[str]]:
    """
    Intenta extraer filas de tablas HTML. Es genérico:
    - busca todas las tablas y toma filas con celdas (>1)
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            # Filas “reales” suelen tener varias columnas
            if len(cells) >= 2:
                rows.append(cells)

    return rows


def extract_worldbank(data_dir: Path, logger):
    """
    Retorna: by_source (dict), durations (dict), errors (list)
    """
    base_url = os.getenv("WORLDBANK_DEBARRED_URL", "").strip()
    max_pages = int(os.getenv("WORLDBANK_MAX_PAGES", "200"))
    timeout_sec = int(os.getenv("WORLDBANK_TIMEOUT_SEC", "30"))

    durations = {}
    errors = []

    if not base_url:
        errors.append({"stage": "extract", "error": "WORLDBANK_DEBARRED_URL no está configurada en .env"})
        return {"status": "SKIPPED"}, durations, errors

    yyyymmdd = time.strftime("%Y%m%d")
    raw_dir = data_dir / "raw" / "worldbank" / yyyymmdd
    html_dir = raw_dir / "html_pages"
    state_path = data_dir / "raw" / "worldbank" / "state_seen_ids.txt"

    session = build_session()
    seen_ids = load_seen_ids(state_path)

    pages_seen = 0
    total_seen = 0
    total_new = 0
    new_ids_to_persist = []
    last_page_hash = None

    t0 = time.time()

    for page in range(1, max_pages + 1):
        # Estrategia de “paginación resiliente”:
        # probamos query params comunes sin depender del sitio.
        # Si el sitio no los soporta, el HTML será igual y cortamos por hash repetido.
        candidate_urls = [
            f"{base_url}?page={page}",
            f"{base_url}?p={page}",
            f"{base_url}?start={(page-1)*50}",
            base_url if page == 1 else None,
        ]
        candidate_urls = [u for u in candidate_urls if u]

        html = None
        used_url = None
        for u in candidate_urls:
            try:
                html = fetch_html(session, u, timeout_sec)
                used_url = u
                break
            except Exception as e:
                # si falla un candidate, probamos el siguiente
                last_err = str(e)
                continue

        if html is None:
            errors.append({"stage": "download", "error": last_err})
            break

        pages_seen += 1
        save_html(html_dir, page, html)

        page_hash = sha256(html)
        if last_page_hash == page_hash:
            # El sitio ignoró la paginación: ya no hay “nuevas páginas”
            break
        last_page_hash = page_hash

        rows = extract_rows_from_html(html)
        if not rows:
            # Probablemente la tabla se carga con JS (no disponible en HTML)
            errors.append({"stage": "parse", "error": "No se encontraron filas en HTML (posible tabla cargada por JS)"})
            break

        total_seen += len(rows)

        # dedupe re-ejecutable
        for row in rows:
            normalized = "|".join([c.strip().lower() for c in row if c is not None])
            rid = sha256(normalized)
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
            new_ids_to_persist.append(rid)
            total_new += 1

        # Si en una “página” no hubo nuevos IDs, es señal fuerte de fin
        if total_new == 0 and page >= 2:
            # no cortamos por esto siempre, pero ayuda
            pass

    durations["scrape_sec"] = round(time.time() - t0, 4)

    if new_ids_to_persist:
        append_seen_ids(state_path, new_ids_to_persist)

    status = "OK" if pages_seen > 0 else "FAILED"
    if errors and status == "OK":
        status = "SUCCESS_WITH_ERRORS"

    by_source = {
        "status": status,
        "pages_seen": pages_seen,
        "total_records_seen": total_seen,
        "total_records_extracted": total_new,
        "raw_html_dir": str(html_dir),
    }

    return by_source, durations, errors