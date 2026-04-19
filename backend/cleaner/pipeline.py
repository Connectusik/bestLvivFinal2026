"""Orchestration: read two files → normalize everything → assemble workbook."""

from __future__ import annotations

import datetime as _dt
import hashlib
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from .detect import run_detectors, summarize, KIND_TITLES, SEVERITY_ORDER
from .io_excel import cadastral_url, read_land, read_realestate, write_workbook
from .normalize import (
    normalize_address,
    normalize_date,
    normalize_float,
    normalize_name,
    normalize_tax_id,
    normalize_text,
)


CLEANER_VERSION = "2.0.0"

APPLIED_TRANSFORMATIONS: list[str] = [
    "ПІБ: латинські літери-двійники (i/I/a/A/c/C/e/E/o/O/p/P/x/X/y/Y/k/K/M/H/B/T) "
    "замінені на відповідні кириличні, коли рядок переважно кириличний.",
    "ПІБ: кожне слово у title-case, дефіси й апострофи зберігаються; "
    "надлишкові пробіли, табуляції і коми прибрано.",
    "Адреси: «вулиця» → «вул.», «будинок» → «буд.», «квартира» → «кв.», "
    "«провулок» → «пров.», «проспект» → «просп.», «площа» → «пл.», "
    "«бульвар» → «бул.», «набережна» → «наб.», «узвіз» → «узв.».",
    "Адреси: «область» → «обл.», «район» → «р-н», «місто» → «м.», «село» → «с.».",
    "Адреси: дубльовані фрагменти через кому згортаються до одного; "
    "«осиротілі» префікси без аргументу в хвості (напр. «…, кв.») видаляються.",
    "Дані: Excel-помилки (#VALUE!, #REF!, #N/A тощо) замінені порожнім рядком.",
    "Податкові номери: лише цифри; довжина поза межами 6–12 → порожньо. "
    "У вихідному файлі колонка з податковим номером явно помічена як "
    "«Текстовий» формат (Excel не конвертує у число).",
    "Тип платника: 8 цифр → юр. особа, 10 цифр → фіз. особа; "
    "назва містить «рада/адміністрація/держ/комунал/управлінн/міністерств» "
    "→ орган влади (перевищує класифікацію за довжиною).",
    "Дати: розпізнаються з форматів %Y-%m-%d, %d.%m.%Y, %d/%m/%Y, %Y/%m/%d, "
    "%d-%m-%Y, або зі справжнього Excel-datetime; зберігаються як дата.",
    "Числа: кома замінюється на крапку, пробіли як розділювачі тисяч "
    "прибираються; нерозпізнаване значення → порожньо.",
    "Частка володіння: якщо відсутня/нерозпізнана → значення 1.0 "
    "(прийнято за замовчуванням як повне володіння) + окрема булева "
    "колонка «Частка невідома» встановлюється в TRUE для аудиту.",
    "Записи «Реєстр власників» агрегуються по податковому номеру; "
    "сумарні площі та вартість округлюються до 4 і 2 знаків відповідно.",
    "Підсвічування змін: клітинка з жовтим фоном містить значення, яке "
    "було змінено нормалізатором. Рядок з блідо-жовтим фоном має хоча б "
    "одну змінену клітинку. Вихідні колонки без прямого відповідника у "
    "вхідних файлах (наприклад, «Статус», «Частка невідома») ніколи не "
    "підсвічуються.",
    "Після нормалізації автоматично запускається пошук розбіжностей: 11 "
    "правил категорій critical/high/medium/low. Результати винесені в "
    "окремий аркуш «Розбіжності» з посиланням на конкретні рядки в "
    "«Земельні ділянки» / «Нерухомість». Для аркуша «Розбіжності» фон "
    "клітинки серйозності — кольорова позначка (критичні = червоний, "
    "високі = помаранчевий, середні = жовтий, низькі = синій).",
    "Кадастрові номери у «Земельних ділянках» та в аркуші «Розбіжності» "
    "(колонка «Кадастрова карта») є гіперпосиланнями на публічний "
    "перегляд ділянки за шаблоном "
    "https://kadastrova-karta.com/dilyanka/{кадастровий_номер}. "
    "Посилання формується лише для значень, що відповідають шаблону "
    "19-значного кадастрового номера (10:2:3:4), тож сміттєві рядки "
    "посилань не створюють.",
]


# ---- Output schemas --------------------------------------------------------

LAND_HEADERS = [
    "Кадастровий номер",
    "КОАТУУ",
    "ПІБ / Назва власника",
    "Податковий номер",
    "Форма власності",
    "Цільове призначення",
    "Адреса / розташування",
    "Вид угідь",
    "Площа, га",
    "Нормативна грошова оцінка, грн",
    "Частка",
    "Частка невідома",
    "Дата реєстрації права",
    "Номер запису",
    "Орган реєстрації",
    "Тип документа",
    "Підтип документа",
]
LAND_WIDTHS = [22, 12, 34, 14, 16, 42, 46, 16, 11, 20, 9, 14, 14, 14, 40, 22, 22]
# 1-based column indexes:
LAND_TEXT_COLS = [2, 4, 14]   # КОАТУУ, Податковий номер, Номер запису
LAND_DATE_COLS = [13]         # Дата реєстрації права
LAND_URL_COLS = {1: cadastral_url}  # col 1 Кадастровий номер → kadastrova-karta.com

RE_HEADERS = [
    "ПІБ / Назва власника",
    "Податковий номер",
    "Тип об'єкта",
    "Адреса об'єкта",
    "Площа, м²",
    "Дата реєстрації",
    "Дата припинення",
    "Вид спільної власності",
    "Розмір частки",
    "Частка невідома",
    "Статус",
]
RE_WIDTHS = [34, 14, 20, 46, 11, 14, 14, 22, 12, 14, 14]
RE_TEXT_COLS = [2]       # Податковий номер
RE_DATE_COLS = [6, 7]    # Дата реєстрації, Дата припинення

OWNER_HEADERS = [
    "Податковий номер",
    "ПІБ / Назва власника",
    "Тип платника",
    "Земельних ділянок",
    "Сумарна площа землі, га",
    "Сумарна оцінка землі, грн",
    "Об'єктів нерухомості",
    "Активних об'єктів",
    "Сумарна площа нерухомості, м²",
    "Адреси нерухомості",
]
OWNER_WIDTHS = [14, 34, 14, 12, 16, 20, 12, 12, 18, 60]
OWNER_TEXT_COLS = [1]  # Податковий номер


# ---- Core cleaning ---------------------------------------------------------


def _share_with_flag(raw_share: Any) -> tuple[float, bool]:
    """Return (share, is_unknown). Missing / unparseable → (1.0, True)."""
    parsed = normalize_float(raw_share)
    if parsed is None:
        return 1.0, True
    return parsed, False


# Mapping from cleaned-column index → raw-column index (or None for derived
# columns that have no direct counterpart in the source data).
_LAND_RAW_MAP: list[int | None] = [
    0,   # 0  кадастровий    ← raw 0
    1,   # 1  КОАТУУ          ← raw 1
    9,   # 2  ПІБ             ← raw 9
    8,   # 3  Податковий номер← raw 8
    2,   # 4  Форма власності ← raw 2
    3,   # 5  Призначення     ← raw 3
    4,   # 6  Адреса          ← raw 4
    5,   # 7  Вид угідь       ← raw 5
    6,   # 8  Площа           ← raw 6
    7,   # 9  Грошова оцінка  ← raw 7
    10,  # 10 Частка          ← raw 10
    None, # 11 Частка невідома (derived)
    11,  # 12 Дата реєстрації ← raw 11
    12,  # 13 Номер запису    ← raw 12
    13,  # 14 Орган реєстрації← raw 13
    14,  # 15 Тип документа   ← raw 14
    15,  # 16 Підтип          ← raw 15
]

_RE_RAW_MAP: list[int | None] = [
    1,   # 0  ПІБ             ← raw 1
    0,   # 1  Податковий номер← raw 0
    2,   # 2  Тип об'єкта     ← raw 2
    3,   # 3  Адреса          ← raw 3
    6,   # 4  Площа           ← raw 6
    4,   # 5  Реєстрація      ← raw 4
    5,   # 6  Припинення      ← raw 5
    7,   # 7  Спільна власність← raw 7
    8,   # 8  Розмір частки   ← raw 8
    None, # 9  Частка невідома (derived)
    None, # 10 Статус (derived from term_date)
]


def _equivalent(raw: Any, cleaned: Any) -> bool:
    """True if the normalizer did not meaningfully change the value.

    Used to build a per-cell "changed" mask for the highlighter in the
    Excel writer. We intentionally treat "None" and empty string as equal
    on both sides so that truly blank cells don't show up as modified.
    """
    raw_empty = raw is None or raw == ""
    cleaned_empty = cleaned is None or cleaned == ""
    if raw_empty and cleaned_empty:
        return True
    if raw_empty or cleaned_empty:
        return False
    # Numeric equivalence (after comma/space cleanup)
    if isinstance(cleaned, (int, float)) and not isinstance(cleaned, bool):
        try:
            raw_num = float(str(raw).replace(",", ".").replace(" ", ""))
        except (TypeError, ValueError):
            return False
        return abs(raw_num - float(cleaned)) < 1e-9
    # Date equivalence
    if isinstance(cleaned, _dt.date) and not isinstance(cleaned, _dt.datetime):
        if isinstance(raw, _dt.datetime):
            return raw.date() == cleaned
        if isinstance(raw, _dt.date):
            return raw == cleaned
        return False
    # String equivalence — trimmed, NFKC in both would be costly here;
    # we rely on the raw already being a plain cell value.
    return str(raw).strip() == str(cleaned).strip()


def _mask(raw: list[Any], cleaned: list[Any], raw_map: list[int | None]) -> list[bool]:
    out = []
    for ci, ri in enumerate(raw_map):
        if ri is None:
            out.append(False)
        else:
            out.append(not _equivalent(raw[ri], cleaned[ci]))
    return out


def _clean_land_row(raw: list[Any]) -> tuple[list[Any], list[bool]]:
    share, share_unknown = _share_with_flag(raw[10])
    cleaned = [
        normalize_text(raw[0]),                  # кадастровий
        normalize_text(raw[1]),                  # КОАТУУ (текстова!)
        normalize_name(raw[9]),                  # ПІБ
        normalize_tax_id(raw[8]),                # Податковий номер
        normalize_text(raw[2]),                  # Форма власності
        normalize_text(raw[3]),                  # Цільове призначення
        normalize_address(raw[4]),               # Адреса
        normalize_text(raw[5]),                  # Вид угідь
        normalize_float(raw[6]),                 # Площа
        normalize_float(raw[7]),                 # Грошова оцінка
        share,                                   # Частка (default 1.0)
        share_unknown,                           # Частка невідома (bool)
        normalize_date(raw[11]),                 # Дата реєстрації
        normalize_text(raw[12]),                 # Номер запису (текстовий)
        normalize_text(raw[13]),                 # Орган реєстрації
        normalize_text(raw[14]),                 # Тип документа
        normalize_text(raw[15]),                 # Підтип
    ]
    return cleaned, _mask(raw, cleaned, _LAND_RAW_MAP)


def _clean_re_row(raw: list[Any]) -> tuple[list[Any], list[bool]]:
    term_date = normalize_date(raw[5])
    share, share_unknown = _share_with_flag(raw[8])
    cleaned = [
        normalize_name(raw[1]),                  # ПІБ
        normalize_tax_id(raw[0]),                # Податковий номер
        normalize_text(raw[2]),                  # Тип об'єкта
        normalize_address(raw[3]),               # Адреса
        normalize_float(raw[6]),                 # Площа
        normalize_date(raw[4]),                  # Реєстрація
        term_date,                               # Припинення
        normalize_text(raw[7]),                  # Вид спільної власності
        share,                                   # Розмір частки (default 1.0)
        share_unknown,                           # Частка невідома (bool)
        "припинено" if term_date else "діє",    # Статус
    ]
    return cleaned, _mask(raw, cleaned, _RE_RAW_MAP)


# ---- Owner roll-up ---------------------------------------------------------


_PUBLIC_KEYWORDS = ("рада", "адміністрація", "держ", "комунал", "управлінн", "міністерств")


def _classify(tax_id: str, name: str = "") -> str:
    """Distinguish a legal entity (ЄДРПОУ, 8 digits) from an individual
    (РНОКПП, 10 digits). Name-based override catches data entry errors
    where a public body ended up with a 10-digit identifier."""
    name_lc = (name or "").lower()
    if any(kw in name_lc for kw in _PUBLIC_KEYWORDS):
        return "орган влади"
    if len(tax_id) == 8:
        return "юр. особа"
    if len(tax_id) == 10:
        return "фіз. особа"
    return "—"


def _build_owners(land: list[list[Any]], realestate: list[list[Any]]) -> list[list[Any]]:
    agg: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "names": set(),
        "land_count": 0,
        "land_area": 0.0,
        "land_value": 0.0,
        "re_count": 0,
        "re_active_count": 0,
        "re_area": 0.0,
        "re_addresses": [],
    })

    for row in land:
        tax_id = row[3]
        if not tax_id:
            continue
        bucket = agg[tax_id]
        if row[2]:
            bucket["names"].add(row[2])
        bucket["land_count"] += 1
        if isinstance(row[8], (int, float)):
            bucket["land_area"] += row[8]
        if isinstance(row[9], (int, float)):
            bucket["land_value"] += row[9]

    for row in realestate:
        tax_id = row[1]
        if not tax_id:
            continue
        bucket = agg[tax_id]
        if row[0]:
            bucket["names"].add(row[0])
        bucket["re_count"] += 1
        if row[10] == "діє":
            bucket["re_active_count"] += 1
        if isinstance(row[4], (int, float)):
            bucket["re_area"] += row[4]
        if row[3] and row[3] not in bucket["re_addresses"]:
            bucket["re_addresses"].append(row[3])

    out: list[list[Any]] = []
    for tax_id, b in agg.items():
        primary_name = sorted(b["names"], key=len, reverse=True)[0] if b["names"] else ""
        addrs = "; ".join(b["re_addresses"][:5])
        if len(b["re_addresses"]) > 5:
            addrs += f" … (+{len(b['re_addresses']) - 5})"
        out.append([
            tax_id,
            primary_name,
            _classify(tax_id, primary_name),
            b["land_count"],
            round(b["land_area"], 4) if b["land_area"] else None,
            round(b["land_value"], 2) if b["land_value"] else None,
            b["re_count"],
            b["re_active_count"],
            round(b["re_area"], 2) if b["re_area"] else None,
            addrs or None,
        ])
    # Sort by land area descending, then name.
    out.sort(key=lambda r: ((r[4] or 0), (r[8] or 0)), reverse=True)
    return out


# ---- Public entry point ----------------------------------------------------


def _file_info(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    data = p.read_bytes()
    return {
        "name": p.name,
        "size": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def clean_to_xlsx(
    land_path: str | Path,
    re_path: str | Path,
    *,
    land_display_name: str | None = None,
    re_display_name: str | None = None,
) -> dict[str, Any]:
    """Run the full pipeline.

    Returns ``{"bytes": xlsx payload, "stats": {...}}``. The optional
    ``*_display_name`` args let the API layer pass the real uploaded filename
    (tempfile names are meaningless in the metadata sheet).
    """
    started = time.perf_counter()

    land_info = _file_info(land_path)
    re_info = _file_info(re_path)

    raw_land = read_land(land_path)
    raw_re = read_realestate(re_path)

    clean_land_pairs = [_clean_land_row(r) for r in raw_land]
    clean_re_pairs = [_clean_re_row(r) for r in raw_re]
    clean_land = [row for row, _ in clean_land_pairs]
    land_masks = [m for _, m in clean_land_pairs]
    clean_re = [row for row, _ in clean_re_pairs]
    re_masks = [m for _, m in clean_re_pairs]
    owners = _build_owners(clean_land, clean_re)

    land_changed_rows = sum(1 for m in land_masks if any(m))
    re_changed_rows = sum(1 for m in re_masks if any(m))

    # Detection runs AFTER cleaning — normalized addresses / names give
    # detectors much better signal than raw data would.
    detect_started = time.perf_counter()
    findings = run_detectors(clean_land, clean_re)
    detect_elapsed = round(time.perf_counter() - detect_started, 2)
    summary = summarize(findings)

    elapsed_read_and_clean = round(time.perf_counter() - started, 2)

    now_utc = _dt.datetime.now(_dt.timezone.utc)
    now_local = _dt.datetime.now().astimezone()

    metadata = {
        "cleaned_at_utc": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "cleaned_at_local": now_local.strftime("%Y-%m-%d %H:%M:%S %Z").strip(),
        "version": CLEANER_VERSION,
        "input_land_name": land_display_name or land_info["name"],
        "input_land_size": land_info["size"],
        "input_land_sha256": land_info["sha256"],
        "input_realestate_name": re_display_name or re_info["name"],
        "input_realestate_size": re_info["size"],
        "input_realestate_sha256": re_info["sha256"],
        "rows_land": len(clean_land),
        "rows_realestate": len(clean_re),
        "rows_owners": len(owners),
        "rows_land_changed": land_changed_rows,
        "rows_realestate_changed": re_changed_rows,
        "elapsed_read_clean_sec": elapsed_read_and_clean,
        "transformations": APPLIED_TRANSFORMATIONS,
        "findings_total": summary["total"],
        "findings_by_severity": summary["by_severity"],
        "findings_exposure_uah": summary["exposure_uah"],
    }

    write_started = time.perf_counter()
    payload = write_workbook(
        owners=owners,
        owners_headers=OWNER_HEADERS,
        owners_widths=OWNER_WIDTHS,
        owners_text_columns=OWNER_TEXT_COLS,
        land=clean_land,
        land_headers=LAND_HEADERS,
        land_widths=LAND_WIDTHS,
        land_text_columns=LAND_TEXT_COLS,
        land_date_columns=LAND_DATE_COLS,
        land_url_columns=LAND_URL_COLS,
        land_changed_masks=land_masks,
        realestate=clean_re,
        re_headers=RE_HEADERS,
        re_widths=RE_WIDTHS,
        re_text_columns=RE_TEXT_COLS,
        re_date_columns=RE_DATE_COLS,
        re_changed_masks=re_masks,
        findings=findings,
        metadata=metadata,
    )

    elapsed_write = round(time.perf_counter() - write_started, 2)
    elapsed_total = round(time.perf_counter() - started, 2)

    stats = {
        "land_rows": len(clean_land),
        "realestate_rows": len(clean_re),
        "owners": len(owners),
        "land_rows_changed": land_changed_rows,
        "realestate_rows_changed": re_changed_rows,
        "bytes": len(payload),
        "elapsed_sec": elapsed_total,
        "elapsed_read_clean_sec": elapsed_read_and_clean,
        "elapsed_write_sec": elapsed_write,
        "elapsed_detect_sec": detect_elapsed,
        "version": CLEANER_VERSION,
        "findings_total": summary["total"],
        "findings_by_severity": summary["by_severity"],
        "findings_by_kind": summary["by_kind"],
        "findings_exposure_uah": summary["exposure_uah"],
    }
    return {"bytes": payload, "stats": stats, "metadata": metadata}
