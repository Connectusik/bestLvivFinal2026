"""Discrepancy detectors — run AFTER ``pipeline._clean_*_row``.

Running on normalized data has a huge practical advantage: addresses,
names and tax IDs are already in canonical form, so detectors don't
waste precision on `"вул." vs "вулиця"` or `"ПЕТРЕНКО" vs "Петренко"`
noise. The whole cleaner-then-detector pipeline is strictly more accurate
than running detectors on raw Excel dumps.

Each detector is a pure function: takes the cleaned dataset (plus a couple
of pre-built indexes), yields ``Finding`` objects. Adding a new rule is
~20 lines and a one-line entry in ``ALL_DETECTORS``.

Severity scale:
    critical — blocks tax collection (no ID, no cadastral)
    high     — likely concrete revenue leak or ownership dispute
    medium   — data-quality issue with some financial impact
    low      — statistical outlier worth a human look

Row indexes in findings are **Excel row numbers** (1-based, with row 1
being the header) so a bookkeeper can paste them directly into Excel's
"Go To" dialog.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from statistics import median
from typing import Any, Iterable

# Column indexes in cleaned rows --------------------------------------------
# land (17): 0 cadastral, 1 koatuu, 2 owner_name, 3 tax_id, 4 ownership_form,
#            5 purpose, 6 address, 7 ag_type, 8 area_ha, 9 value_uah,
#            10 share, 11 share_unknown, 12 reg_date, 13 record_num,
#            14 registrar, 15 type, 16 subtype
# re (11):   0 owner_name, 1 tax_id, 2 object_type, 3 address, 4 area,
#            5 reg_date, 6 term_date, 7 joint_type, 8 share_size,
#            9 share_unknown, 10 status ("діє"/"припинено")


@dataclass(slots=True)
class Finding:
    kind: str
    severity: str
    title: str
    description: str
    evidence: dict[str, Any] = field(default_factory=dict)
    land_excel_rows: list[int] = field(default_factory=list)
    re_excel_rows: list[int] = field(default_factory=list)
    owner_id: str = ""
    owner_name: str = ""
    # financial exposure (if meaningful) — used to compute UAH-at-risk
    exposure_uah: float = 0.0


# ---------------------------------------------------------------------------
# Indexes
# ---------------------------------------------------------------------------


@dataclass
class Index:
    land: list[list[Any]]
    realestate: list[list[Any]]
    land_by_owner: dict[str, list[tuple[int, list[Any]]]]
    re_by_owner: dict[str, list[tuple[int, list[Any]]]]
    re_by_address: dict[str, list[tuple[int, list[Any]]]]
    land_by_cadastral: dict[str, list[tuple[int, list[Any]]]]


def build_index(land: list[list[Any]], realestate: list[list[Any]]) -> Index:
    land_by_owner: dict[str, list[tuple[int, list[Any]]]] = defaultdict(list)
    land_by_cadastral: dict[str, list[tuple[int, list[Any]]]] = defaultdict(list)
    re_by_owner: dict[str, list[tuple[int, list[Any]]]] = defaultdict(list)
    re_by_address: dict[str, list[tuple[int, list[Any]]]] = defaultdict(list)

    # Excel row = data index + 2 (one for 0/1-based switch, one for header).
    for i, row in enumerate(land):
        excel_row = i + 2
        if row[3]:
            land_by_owner[row[3]].append((excel_row, row))
        if row[0]:
            land_by_cadastral[row[0]].append((excel_row, row))

    for i, row in enumerate(realestate):
        excel_row = i + 2
        if row[1]:
            re_by_owner[row[1]].append((excel_row, row))
        if row[3]:
            re_by_address[row[3]].append((excel_row, row))

    return Index(
        land=land,
        realestate=realestate,
        land_by_owner=land_by_owner,
        land_by_cadastral=land_by_cadastral,
        re_by_owner=re_by_owner,
        re_by_address=re_by_address,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_KOATUU_OBLAST_KEYWORDS = {
    "46": "львівська",
    "05": "вінницька",
    "07": "волинська",
    "21": "закарпатська",
    "26": "івано-франківська",
    "61": "тернопільська",
    "68": "хмельницька",
}

_PUBLIC_KEYWORDS = ("рада", "адміністрація", "управлінн", "міністерств", "держ", "комунал")

_RESIDENTIAL_PURPOSE_KEYWORDS = (
    "житлов", "присадибна", "індивідуальног", "садівництва", "гаражного", "дачног",
)


def _is_public_owner(name: str) -> bool:
    n = (name or "").lower()
    return any(kw in n for kw in _PUBLIC_KEYWORDS)


# ---------------------------------------------------------------------------
# Detectors
# ---------------------------------------------------------------------------


def detect_missing_cadastral(ix: Index) -> Iterable[Finding]:
    for i, row in enumerate(ix.land):
        if not row[0]:
            yield Finding(
                kind="missing_cadastral",
                severity="critical",
                title="Немає кадастрового номера",
                description=(
                    "Запис у земельному реєстрі без кадастрового номера — "
                    "об'єкт неможливо ідентифікувати в ДЗК і нарахувати "
                    "земельний податок."
                ),
                evidence={
                    "owner_name": row[2],
                    "address": row[6],
                    "area_ha": row[8],
                },
                land_excel_rows=[i + 2],
                owner_id=row[3] or "",
                owner_name=row[2] or "",
                exposure_uah=row[9] or 0.0,
            )


def detect_missing_owner_id(ix: Index) -> Iterable[Finding]:
    for i, row in enumerate(ix.land):
        if not row[3]:
            yield Finding(
                kind="missing_owner_id",
                severity="critical",
                title="У земельному реєстрі відсутній ідентифікатор власника",
                description=(
                    "Запис не містить ЄДРПОУ/РНОКПП землекористувача — "
                    "неможливо встановити платника податку."
                ),
                evidence={
                    "cadastral": row[0],
                    "location": row[6],
                    "owner_name_raw": row[2],
                },
                land_excel_rows=[i + 2],
                owner_name=row[2] or "",
                exposure_uah=row[9] or 0.0,
            )


def detect_duplicate_cadastral(ix: Index) -> Iterable[Finding]:
    for cadastral, entries in ix.land_by_cadastral.items():
        if len(entries) < 2:
            continue
        distinct = {row[3] for _, row in entries if row[3]}
        if len(distinct) < 2:
            continue
        yield Finding(
            kind="duplicate_cadastral",
            severity="critical",
            title="Дубль кадастрового номера з різними власниками",
            description=(
                "Одна земельна ділянка фігурує в реєстрі кілька разів з "
                "різними власниками — класичний маркер подвійної реєстрації."
            ),
            evidence={
                "cadastral": cadastral,
                "owners_count": len(distinct),
                "owners": [
                    {"id": row[3], "name": row[2]}
                    for _, row in entries
                ],
            },
            land_excel_rows=[r for r, _ in entries],
            owner_name=entries[0][1][2] or "",
            exposure_uah=sum((r[9] or 0.0) for _, r in entries),
        )


def detect_missing_area(ix: Index) -> Iterable[Finding]:
    for i, row in enumerate(ix.land):
        if row[8] is None:
            yield Finding(
                kind="missing_area",
                severity="high",
                title="Земельна ділянка без зазначеної площі",
                description=(
                    "Неможливо нарахувати земельний податок чи орендну плату: "
                    "поле «Площа» не заповнене."
                ),
                evidence={
                    "cadastral": row[0],
                    "owner_name": row[2],
                    "value_uah": row[9],
                },
                land_excel_rows=[i + 2],
                owner_id=row[3] or "",
                owner_name=row[2] or "",
                exposure_uah=row[9] or 0.0,
            )


def detect_residential_no_building(ix: Index) -> Iterable[Finding]:
    """Residential-purpose land parcel whose owner has zero registered real
    estate. Strong indicator of an undeclared building — direct property-tax
    leakage.
    """
    for owner_id, entries in ix.land_by_owner.items():
        if owner_id in ix.re_by_owner:
            continue
        owner_name = entries[0][1][2] or ""
        if _is_public_owner(owner_name):
            continue
        residential = [
            (r, row) for r, row in entries
            if row[5] and any(kw in row[5].lower() for kw in _RESIDENTIAL_PURPOSE_KEYWORDS)
        ]
        if not residential:
            continue
        total_area = sum((row[8] or 0.0) for _, row in residential)
        total_value = sum((row[9] or 0.0) for _, row in residential)
        yield Finding(
            kind="residential_no_building",
            severity="high",
            title="Житлова ділянка без зареєстрованої нерухомості",
            description=(
                f"«{owner_name}» володіє земельною ділянкою з цільовим "
                f"призначенням під забудову ({residential[0][1][5][:60]}…), "
                "але в реєстрі прав на нерухомість жодного об'єкта немає. "
                "Ймовірна незадекларована будівля — пряма втрата податку на "
                "нерухоме майно."
            ),
            evidence={
                "parcels_count": len(residential),
                "total_area_ha": round(total_area, 4),
                "sample_cadastrals": [row[0] for _, row in residential[:5]],
                "sample_addresses": list({row[6] for _, row in residential[:5] if row[6]})[:3],
            },
            land_excel_rows=[r for r, _ in residential],
            owner_id=owner_id,
            owner_name=owner_name,
            exposure_uah=total_value,
        )


def detect_name_mismatch(ix: Index) -> Iterable[Finding]:
    """Same tax ID but different normalized names in the two registries.

    After the normalizer, names only differ when they really are different —
    false positives from casing/latin-contamination are already gone."""
    for owner_id, land_entries in ix.land_by_owner.items():
        re_entries = ix.re_by_owner.get(owner_id)
        if not re_entries:
            continue
        land_names = {row[2] for _, row in land_entries if row[2]}
        re_names = {row[0] for _, row in re_entries if row[0]}
        if not land_names or not re_names:
            continue
        if land_names & re_names:
            continue
        yield Finding(
            kind="name_mismatch",
            severity="high",
            title="Різне ПІБ за одним ідентифікатором",
            description=(
                "За одним податковим номером в двох реєстрах зазначено "
                "різні ПІБ — ймовірна помилка ДРРП або зміна прізвища без "
                "оновлення одного з реєстрів."
            ),
            evidence={
                "land_names": sorted(land_names),
                "realestate_names": sorted(re_names),
            },
            land_excel_rows=[r for r, _ in land_entries],
            re_excel_rows=[r for r, _ in re_entries],
            owner_id=owner_id,
            owner_name=next(iter(land_names)),
            exposure_uah=sum((row[9] or 0.0) for _, row in land_entries),
        )


def detect_koatuu_address_mismatch(ix: Index) -> Iterable[Finding]:
    for i, row in enumerate(ix.land):
        koatuu = row[1]
        address = row[6]
        if not koatuu or not address:
            continue
        prefix = str(koatuu)[:2]
        expected = _KOATUU_OBLAST_KEYWORDS.get(prefix)
        if expected is None:
            continue
        if expected not in address.lower():
            yield Finding(
                kind="koatuu_address_mismatch",
                severity="medium",
                title="КОАТУУ не відповідає області в адресі",
                description=(
                    f"Код КОАТУУ починається на {prefix} (очікувана область "
                    f"— {expected.title()}), але у полі «Адреса» цієї області "
                    "немає."
                ),
                evidence={
                    "koatuu": koatuu,
                    "expected_oblast": expected,
                    "address": address,
                    "cadastral": row[0],
                },
                land_excel_rows=[i + 2],
                owner_id=row[3] or "",
                owner_name=row[2] or "",
                exposure_uah=0.0,
            )


def detect_public_owner_as_private(ix: Index) -> Iterable[Finding]:
    for i, row in enumerate(ix.land):
        form = (row[4] or "").lower()
        if "приват" not in form:
            continue
        if _is_public_owner(row[2]):
            yield Finding(
                kind="public_owner_as_private",
                severity="medium",
                title="Орган влади зареєстровано на ділянку як «Приватна»",
                description=(
                    "Форма власності — «Приватна», але землекористувач — "
                    "орган місцевого самоврядування або державна структура. "
                    "Ймовірна некоректна класифікація форми власності."
                ),
                evidence={
                    "cadastral": row[0],
                    "owner_name": row[2],
                    "ownership_form": row[4],
                },
                land_excel_rows=[i + 2],
                owner_id=row[3] or "",
                owner_name=row[2] or "",
                exposure_uah=row[9] or 0.0,
            )


def detect_share_overflow(ix: Index) -> Iterable[Finding]:
    for address, entries in ix.re_by_address.items():
        if len(entries) < 2:
            continue
        active = [(r, row) for r, row in entries if row[10] == "діє"]
        if len(active) < 2:
            continue
        # Ignore cases where any share was defaulted to 1.0 unknown — that's
        # our own fill, not the source data.
        if any(row[9] for _, row in active):
            continue
        total_area = max((row[4] or 0.0) for _, row in entries)
        if total_area <= 0:
            continue
        share_sum = sum((row[8] or 0.0) for _, row in active)
        if share_sum <= total_area * 1.01:
            continue
        yield Finding(
            kind="share_overflow",
            severity="medium",
            title="Сума часток перевищує загальну площу об'єкта",
            description=(
                "За однією адресою сумарна частка власників у праві "
                "спільної власності перевищує зареєстровану загальну площу."
            ),
            evidence={
                "address": address,
                "total_area": total_area,
                "sum_of_shares": round(share_sum, 2),
                "owners_count": len(active),
            },
            re_excel_rows=[r for r, _ in active],
            exposure_uah=0.0,
        )


def detect_value_outlier(ix: Index) -> Iterable[Finding]:
    buckets: dict[str, list[float]] = defaultdict(list)
    for row in ix.land:
        if row[9] and row[8] and row[8] > 0 and row[5]:
            buckets[row[5]].append(row[9] / row[8])
    medians = {p: median(v) for p, v in buckets.items() if len(v) >= 20}

    for i, row in enumerate(ix.land):
        if not (row[9] and row[8] and row[8] > 0 and row[5]):
            continue
        m = medians.get(row[5])
        if not m:
            continue
        per_ha = row[9] / row[8]
        if per_ha > m * 10 or per_ha < m / 10:
            yield Finding(
                kind="value_outlier",
                severity="low",
                title="Нестандартна нормативна грошова оцінка",
                description=(
                    "Вартість за гектар у цього запису відхиляється від "
                    "медіани по цій категорії використання більш ніж у 10 "
                    "разів — імовірна помилка в оцінці або площі."
                ),
                evidence={
                    "cadastral": row[0],
                    "purpose": row[5],
                    "value_uah": row[9],
                    "area_ha": row[8],
                    "value_per_ha": round(per_ha, 2),
                    "median_per_ha": round(m, 2),
                },
                land_excel_rows=[i + 2],
                owner_id=row[3] or "",
                owner_name=row[2] or "",
                exposure_uah=0.0,
            )


def detect_ancient_reg_date(ix: Index) -> Iterable[Finding]:
    for i, row in enumerate(ix.realestate):
        d = row[5]
        if d is not None and hasattr(d, "year") and d.year < 1991:
            yield Finding(
                kind="ancient_reg_date",
                severity="low",
                title="Нереалістична дата реєстрації права",
                description=(
                    "ДРРП діє з 2013 року; дата до 1991 р. однозначно — "
                    "помилка введення."
                ),
                evidence={
                    "reg_date": str(d),
                    "owner_name": row[0],
                    "address": row[3],
                },
                re_excel_rows=[i + 2],
                owner_id=row[1] or "",
                owner_name=row[0] or "",
                exposure_uah=0.0,
            )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


ALL_DETECTORS: tuple = (
    detect_missing_cadastral,
    detect_missing_owner_id,
    detect_duplicate_cadastral,
    detect_missing_area,
    detect_residential_no_building,
    detect_name_mismatch,
    detect_koatuu_address_mismatch,
    detect_public_owner_as_private,
    detect_share_overflow,
    detect_value_outlier,
    detect_ancient_reg_date,
)


KIND_TITLES: dict[str, str] = {
    "missing_cadastral": "Немає кадастрового номера",
    "missing_owner_id": "Немає ідентифікатора власника",
    "duplicate_cadastral": "Дубль кадастрового номера",
    "missing_area": "Земля без площі",
    "residential_no_building": "Житлова ділянка без будівлі",
    "name_mismatch": "Різне ПІБ за одним ID",
    "koatuu_address_mismatch": "КОАТУУ ≠ область в адресі",
    "public_owner_as_private": "Орган влади як «Приватна»",
    "share_overflow": "Сума часток > площі",
    "value_outlier": "Аномальна грошова оцінка",
    "ancient_reg_date": "Нереалістична дата реєстрації",
}

SEVERITY_ORDER = ("critical", "high", "medium", "low")


def run_detectors(land: list[list[Any]], realestate: list[list[Any]]) -> list[Finding]:
    ix = build_index(land, realestate)
    out: list[Finding] = []
    for fn in ALL_DETECTORS:
        out.extend(fn(ix))
    out.sort(key=lambda f: (SEVERITY_ORDER.index(f.severity), f.kind, f.owner_name))
    return out


def summarize(findings: list[Finding]) -> dict[str, Any]:
    by_severity: dict[str, int] = defaultdict(int)
    by_kind: dict[str, int] = defaultdict(int)
    total_exposure = 0.0
    for f in findings:
        by_severity[f.severity] += 1
        by_kind[f.kind] += 1
        total_exposure += f.exposure_uah
    return {
        "total": len(findings),
        "by_severity": dict(by_severity),
        "by_kind": [
            {"kind": k, "title": KIND_TITLES.get(k, k), "count": c}
            for k, c in sorted(by_kind.items(), key=lambda kv: -kv[1])
        ],
        "exposure_uah": round(total_exposure, 2),
    }
