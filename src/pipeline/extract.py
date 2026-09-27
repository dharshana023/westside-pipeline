"""
Extraction logic. `fetch_products` takes an injectable `session` (anything with a
`.get()` method — `requests`, a `requests.Session`, or a test double) so tests never
need a real network call: they pass a mock session instead.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable

log = logging.getLogger(__name__)

DEFAULT_API_URL = "https://fakestoreapi.com/products"


class ExtractionError(Exception):
    """Raised when the API could not be reached after all retries and no
    fallback was supplied."""


def fetch_products(
    session,
    api_url: str = DEFAULT_API_URL,
    timeout_s: int = 8,
    retries: int = 3,
    backoff_base_s: float = 0.5,
    sleep: Callable[[float], None] = time.sleep,
    fallback: list[dict] | None = None,
) -> list[dict]:
    """Extract raw product records from the source REST API.

    `session` is injected (e.g. the `requests` module, a `requests.Session()`, or a
    mock in tests) so this function has no hidden global dependency and no test
    needs real network access.
    """
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(api_url, timeout=timeout_s)
            resp.raise_for_status()
            records = resp.json()
            log.info("Extracted %d records from %s", len(records), api_url)
            return records
        except Exception as e:  # noqa: BLE001 - deliberately broad: any transport error retries
            last_error = e
            log.warning("Extract attempt %d/%d failed: %s", attempt, retries, e)
            if attempt < retries:
                sleep(backoff_base_s * (2 ** (attempt - 1)))

    if fallback is not None:
        log.warning("All %d attempts failed; using fallback dataset (%d records).", retries, len(fallback))
        return fallback

    raise ExtractionError(f"Failed to extract from {api_url} after {retries} attempts") from last_error
