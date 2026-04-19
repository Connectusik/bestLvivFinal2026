"""КОАТУУ reference: oblast + rayon lookup by numeric prefix.

Data flow:
1. Bundled JSON loads on first access (always works, no network).
2. If ``KOATUU_REFRESH_URL`` env var is set, best-effort fetch is attempted —
   the expected payload is JSON with the same shape as the bundled files.
   Any error (timeout, HTTP, malformed JSON) is swallowed and bundled stays.

The bundled dataset is deliberately Lviv-oblast-heavy because the pilot
(Червоноградська ОТГ) sits there. Other regions are covered at oblast level
only — sufficient for `koatuu_address_mismatch` at the country scale and
easy to extend by dropping more rayons into ``data/koatuu_rayons.json``.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import urllib.request
from pathlib import Path
from typing import Any

_DATA_DIR = Path(__file__).parent / "data"


class KoatuuRef:
    def __init__(self) -> None:
        self._oblasts: dict[str, str] = {}
        self._oblast_keywords: dict[str, list[str]] = {}
        self._rayons: dict[str, str] = {}
        self._loaded_from: str = "none"
        self._loaded_at: str | None = None
        self._source_version: str | None = None

    # ---- loading ----------------------------------------------------------

    def load_bundled(self) -> None:
        oblasts = json.loads((_DATA_DIR / "koatuu_oblasts.json").read_text(encoding="utf-8"))
        rayons = json.loads((_DATA_DIR / "koatuu_rayons.json").read_text(encoding="utf-8"))
        self._oblasts = oblasts["oblasts"]
        self._oblast_keywords = {k: [kw.lower() for kw in v] for k, v in oblasts["address_keywords"].items()}
        self._rayons = rayons["rayons"]
        self._loaded_from = "bundled"
        self._loaded_at = _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")
        self._source_version = oblasts.get("version", "unknown")

    def try_refresh(self, url: str | None = None, timeout: float = 3.0) -> bool:
        url = url or os.environ.get("KOATUU_REFRESH_URL")
        if not url:
            return False
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            if "oblasts" in payload:
                self._oblasts = payload["oblasts"]
            if "address_keywords" in payload:
                self._oblast_keywords = {
                    k: [kw.lower() for kw in v] for k, v in payload["address_keywords"].items()
                }
            if "rayons" in payload:
                self._rayons = payload["rayons"]
            self._loaded_from = f"remote:{url}"
            self._loaded_at = _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")
            self._source_version = payload.get("version", self._source_version)
            return True
        except Exception:
            # Silent fallback — bundled data stays in place.
            return False

    # ---- lookups ----------------------------------------------------------

    def oblast_name(self, koatuu: Any) -> str | None:
        if not koatuu:
            return None
        return self._oblasts.get(str(koatuu)[:2])

    def oblast_keywords(self, koatuu: Any) -> list[str]:
        if not koatuu:
            return []
        return self._oblast_keywords.get(str(koatuu)[:2], [])

    def rayon_name(self, koatuu: Any) -> str | None:
        if not koatuu:
            return None
        return self._rayons.get(str(koatuu)[:5])

    # ---- introspection ---------------------------------------------------

    def status(self) -> dict[str, Any]:
        return {
            "source": "КОАТУУ — Класифікатор адміністративно-територіального устрою",
            "loaded_from": self._loaded_from,
            "loaded_at": self._loaded_at,
            "version": self._source_version,
            "oblasts": len(self._oblasts),
            "rayons": len(self._rayons),
        }


_instance: KoatuuRef | None = None


def koatuu() -> KoatuuRef:
    global _instance
    if _instance is None:
        _instance = KoatuuRef()
        _instance.load_bundled()
        _instance.try_refresh()
    return _instance
