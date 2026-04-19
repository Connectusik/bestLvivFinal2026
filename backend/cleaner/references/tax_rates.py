"""Tax rate provider — projected-loss calculator for the exposure metric.

Before this module existed, `exposure_uah` was the raw cadastral valuation of
parcels that landed in findings. That's an interesting number but not
actionable — no one pays 100 % of the valuation as tax. We now project the
annual tax that the community stands to lose if the finding isn't fixed, using
ПКУ-default rates with per-oblast overrides.

Bundled defaults live in ``data/tax_rates_default.json`` and can be overridden
by setting ``TAX_RATES_REFRESH_URL`` to a JSON endpoint with the same shape.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import urllib.request
from pathlib import Path
from typing import Any

_DATA_DIR = Path(__file__).parent / "data"


class TaxRates:
    def __init__(self) -> None:
        self._defaults: dict[str, float] = {}
        self._overrides_by_oblast: dict[str, dict[str, float]] = {}
        self._minimum_wage: float = 0.0
        self._notes: list[str] = []
        self._loaded_from: str = "none"
        self._loaded_at: str | None = None
        self._source_version: str | None = None

    def load_bundled(self) -> None:
        raw = json.loads((_DATA_DIR / "tax_rates_default.json").read_text(encoding="utf-8"))
        self._apply(raw)
        self._loaded_from = "bundled"
        self._loaded_at = _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")

    def try_refresh(self, url: str | None = None, timeout: float = 3.0) -> bool:
        url = url or os.environ.get("TAX_RATES_REFRESH_URL")
        if not url:
            return False
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                raw = json.loads(resp.read().decode("utf-8"))
            self._apply(raw)
            self._loaded_from = f"remote:{url}"
            self._loaded_at = _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")
            return True
        except Exception:
            return False

    def _apply(self, raw: dict[str, Any]) -> None:
        self._defaults = dict(raw.get("defaults") or {})
        self._overrides_by_oblast = dict(raw.get("overrides_by_oblast") or {})
        self._minimum_wage = float(raw.get("minimum_wage_uah") or 0.0)
        self._notes = list(raw.get("notes") or [])
        self._source_version = raw.get("version")

    # ---- rate lookups ----------------------------------------------------

    def _land_rate_pct(self, koatuu: Any) -> float:
        base = self._defaults.get("land_tax_rate_pct_of_value", 1.0)
        if not koatuu:
            return base
        oblast_override = self._overrides_by_oblast.get(str(koatuu)[:2]) or {}
        return oblast_override.get("land_tax_rate_pct_of_value", base)

    # ---- projection helpers ---------------------------------------------

    def project_land_tax(
        self,
        value_uah: float | None,
        share: float | None,
        koatuu: Any = None,
    ) -> float:
        """Projected ANNUAL land tax in UAH — the single leverage point for
        findings that make the parcel effectively untaxable."""
        if not value_uah or value_uah <= 0:
            return 0.0
        rate = self._land_rate_pct(koatuu) / 100.0
        s = share if share is not None else 1.0
        return round(float(value_uah) * rate * float(s), 2)

    def project_property_tax(
        self,
        area_m2: float | None,
        object_type: str | None,
        share: float | None,
    ) -> float:
        """Projected ANNUAL property tax for suspected undeclared residential
        object. Uses the above-threshold excess-m² rate from ПКУ 266.5.1."""
        if not area_m2 or area_m2 <= 0:
            return 0.0
        excess_rate = self._defaults.get("property_excess_m2_rate_uah", 120.0)
        t = (object_type or "").lower()
        if "будин" in t or "house" in t:
            free = self._defaults.get("property_free_m2_house", 120.0)
        elif "квартир" in t:
            free = self._defaults.get("property_free_m2_apartment", 60.0)
        else:
            free = 0.0
        taxable = max(float(area_m2) - free, 0.0)
        if taxable <= 0:
            return 0.0
        s = share if share is not None else 1.0
        return round(taxable * excess_rate * float(s), 2)

    # ---- introspection ---------------------------------------------------

    def status(self) -> dict[str, Any]:
        return {
            "source": "Податковий кодекс України, ст. 266 і 274",
            "loaded_from": self._loaded_from,
            "loaded_at": self._loaded_at,
            "version": self._source_version,
            "minimum_wage_uah": self._minimum_wage,
            "defaults": dict(self._defaults),
            "overrides_count": len(self._overrides_by_oblast),
        }


_instance: TaxRates | None = None


def tax_rates() -> TaxRates:
    global _instance
    if _instance is None:
        _instance = TaxRates()
        _instance.load_bundled()
        _instance.try_refresh()
    return _instance
