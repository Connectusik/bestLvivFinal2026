"""External reference data providers: КОАТУУ, tax rates, ЄДРПОУ validation.

Design:
- Each provider ships a bundled JSON fallback so the pipeline runs fully offline.
- A provider may try to refresh from a configurable URL (env-var driven) at
  startup; failures are swallowed silently and the bundled data stays in place.
- Singletons are built on first access via ``koatuu()``, ``tax_rates()``.

This is what lets the pitch claim "we integrate with open data" without
sacrificing the on-premise privacy story — if the VPS has no internet, we
still give correct answers from seeded data.
"""

from .koatuu import KoatuuRef, koatuu
from .tax_rates import TaxRates, tax_rates
from .edrpou import edrpou_checksum_valid, rnokpp_checksum_valid

__all__ = [
    "KoatuuRef",
    "TaxRates",
    "koatuu",
    "tax_rates",
    "edrpou_checksum_valid",
    "rnokpp_checksum_valid",
]
