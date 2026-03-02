import re
import json
import hashlib
from datetime import datetime
from typing import Any, Dict, Optional
import time
import logging
import requests
import unicodedata

def norm_text(x: Any) -> Optional[str]:
    if x is None:
        return None

    s = str(x).strip()

    if not s:
        return None

    # Normalizar unicode (NFKD)
    s = unicodedata.normalize("NFKD", s)

    # Eliminar tildes / diacríticos
    s = "".join(c for c in s if not unicodedata.combining(c))

    # Convertir a mayúsculas
    s = s.upper()

    # Reemplazar múltiples espacios
    s = re.sub(r"\s+", " ", s)

    # Opcional: eliminar caracteres no alfanuméricos extremos
    # s = re.sub(r"[^\w\s\-\.,/]", "", s)

    return s if s else None


def parse_date_iso(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None

    # formatos típicos OFAC
    for fmt in ("%Y-%m-%d", "%d %b %Y", "%d %B %Y", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass

    # intento ISO
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return None


def stable_hash_from_dict(obj: Dict[str, Any]) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def make_id_registro(fuente: str, hash_contenido: str) -> str:
    return f"{fuente}:{hash_contenido[:24]}"

class HTTPClient:
    """
    Cliente HTTP simple con:
    - retries
    - backoff exponencial
    - timeout configurable
    - logging estructurado
    """

    def __init__(
        self,
        logger: logging.Logger,
        timeout_connect: int = 10,
        timeout_read: int = 60,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        self.logger = logger
        self.timeout = (timeout_connect, timeout_read)
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def get(self, url: str, stream: bool = True) -> requests.Response:
        last_exception: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(f"[HTTP] Attempt {attempt}/{self.max_retries} GET {url}")

                response = requests.get(
                    url,
                    stream=stream,
                    timeout=self.timeout,
                    headers={
                        "User-Agent": "Mozilla/5.0 (DataPipelineBot/1.0)"
                    },
                )

                # Errores HTTP
                if 400 <= response.status_code < 500:
                    # Error cliente → no vale la pena reintentar
                    self.logger.error(f"[HTTP] Client error {response.status_code}")
                    response.raise_for_status()

                if 500 <= response.status_code < 600:
                    raise requests.HTTPError(
                        f"Server error {response.status_code}"
                    )

                return response

            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
                last_exception = e
                self.logger.warning(f"[HTTP] Attempt {attempt} failed: {str(e)}")

                if attempt < self.max_retries:
                    sleep_time = self.backoff_factor ** attempt
                    self.logger.info(f"[HTTP] Retrying in {sleep_time:.1f}s...")
                    time.sleep(sleep_time)
                else:
                    break

        raise last_exception