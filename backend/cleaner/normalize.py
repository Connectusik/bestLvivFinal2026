"""Text normalizers for ПІБ, addresses and tax IDs.

All functions accept any-type input and always return a string (possibly
empty). They are pure, so a bookkeeper can reason about each
transformation without reading through the rest of the codebase.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime
from typing import Any

# ---------------------------------------------------------------------------
# Common cleanup
# ---------------------------------------------------------------------------

_WS_RE = re.compile(r"\s+")
_COMMA_RE = re.compile(r"\s*,\s*")
_EXCEL_ERROR_RE = re.compile(r"#\w+!", re.IGNORECASE)


def _base_clean(val: Any) -> str:
    if val is None:
        return ""
    s = str(val)
    s = unicodedata.normalize("NFKC", s)
    if _EXCEL_ERROR_RE.fullmatch(s.strip()):
        return ""
    s = _WS_RE.sub(" ", s).strip(" ,;")
    return s


# ---------------------------------------------------------------------------
# Latin → Cyrillic substitution (only when the string is mostly Cyrillic).
# Needed because ДРРП sometimes stores "Iванович" with a Latin capital I.
# ---------------------------------------------------------------------------

_LATIN_TO_CYR = str.maketrans({
    "a": "а", "A": "А",
    "c": "с", "C": "С",
    "e": "е", "E": "Е",
    "i": "і", "I": "І",
    "o": "о", "O": "О",
    "p": "р", "P": "Р",
    "x": "х", "X": "Х",
    "y": "у", "Y": "У",
    "k": "к", "K": "К",
    "M": "М", "H": "Н",
    "B": "В", "T": "Т",
    "b": "ь",  # rare but happens
})

_CYR_LETTER_RE = re.compile(r"[А-Яа-яЁёЇїІіЄєҐґ]")
_LAT_LETTER_RE = re.compile(r"[A-Za-z]")


def fix_latin_contamination(s: str) -> str:
    """Replace Latin look-alike chars with Cyrillic equivalents, but only
    when the string is predominantly Cyrillic to avoid mangling pure English
    values like "OK" or "N/A"."""
    if not s:
        return s
    cyr = len(_CYR_LETTER_RE.findall(s))
    lat = len(_LAT_LETTER_RE.findall(s))
    if cyr == 0 or lat == 0:
        return s
    if cyr > lat:
        return s.translate(_LATIN_TO_CYR)
    return s


# ---------------------------------------------------------------------------
# ПІБ
# ---------------------------------------------------------------------------

_NAME_SEP_RE = re.compile(r"[\s]+")


def _title_word(word: str) -> str:
    """Title-case a single word, preserving internal hyphens and apostrophes.

    ПІБ normalization always uses proper title-case — "ПЕТРЕНКО" → "Петренко".
    """
    if not word:
        return word
    parts = re.split(r"([-'ʼ’])", word)
    out = []
    for p in parts:
        if p in ("-", "'", "ʼ", "’"):
            out.append(p)
        elif p:
            out.append(p[:1].upper() + p[1:].lower())
    return "".join(out)


def normalize_name(val: Any) -> str:
    """Return ПІБ / назва юрособи in a consistent, title-cased form."""
    s = _base_clean(val)
    if not s:
        return ""
    s = fix_latin_contamination(s)
    # Keep only letters, digits, spaces, apostrophes, hyphens, dots, quotes.
    s = re.sub(r'[^\w \-\'\.\"ʼ’№–—()]+', " ", s, flags=re.UNICODE)
    s = _NAME_SEP_RE.sub(" ", s).strip()
    if not s:
        return ""
    return " ".join(_title_word(w) for w in s.split(" "))


# ---------------------------------------------------------------------------
# Address
# ---------------------------------------------------------------------------

# Order matters — longer patterns first so "вулиця" doesn't match as "вул".
_ADDR_SUBSTITUTIONS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b(вулиц[яіїю]|вул(?:\.|\s|$))", re.I), "вул. "),
    (re.compile(r"\b(провулок|провул(?:\.|\s|$)|пров(?:\.|\s|$))", re.I), "пров. "),
    (re.compile(r"\b(проспект|просп(?:\.|\s|$)|пр-т\s*)", re.I), "просп. "),
    (re.compile(r"\b(площ[аиіу]|пл(?:\.|\s|$))", re.I), "пл. "),
    (re.compile(r"\b(бульвар|бульв(?:\.|\s|$)|бул(?:\.|\s|$))", re.I), "бул. "),
    (re.compile(r"\b(узвіз|узв(?:\.|\s|$))", re.I), "узв. "),
    (re.compile(r"\b(набережна|наб(?:\.|\s|$))", re.I), "наб. "),
    (re.compile(r"\b(шосе)\b", re.I), "шосе "),
    (re.compile(r"\b(місто|м(?:\.|\s))", re.I), "м. "),
    (re.compile(r"\b(смт\.?|селище міського типу)\b", re.I), "смт "),
    (re.compile(r"\b(село|с(?:\.|\s))", re.I), "с. "),
    (re.compile(r"\b(область|обл(?:\.|\s|$))", re.I), "обл. "),
    (re.compile(r"\b(район|р-н|рай(?:\.|\s|$))", re.I), "р-н "),
    (re.compile(r"\b(будинок|буд(?:\.|\s|$)|б\.)\b", re.I), "буд. "),
    (re.compile(r"\b(квартира|кварт(?:\.|\s|$)|кв(?:\.|\s|$))", re.I), "кв. "),
    (re.compile(r"\b(корпус|корп(?:\.|\s|$)|к(?:\.|\s|$))", re.I), "корп. "),
    (re.compile(r"\b(гараж[а-яі]*)\b", re.I), "гараж "),
]

# Repeated fragments "вул. Х, буд. 1, вул. Х, буд. 1" → collapse
def _collapse_repeats(s: str) -> str:
    parts = [p.strip() for p in s.split(",")]
    seen: list[str] = []
    for p in parts:
        if p and p not in seen:
            seen.append(p)
    return ", ".join(seen)


def normalize_address(val: Any) -> str:
    s = _base_clean(val)
    if not s:
        return ""
    s = fix_latin_contamination(s)
    s = s.replace("№", "№ ")
    for pat, repl in _ADDR_SUBSTITUTIONS:
        s = pat.sub(repl, s)
    # Collapse multi-spaces again after substitutions
    s = _WS_RE.sub(" ", s)
    s = _COMMA_RE.sub(", ", s)
    # Title-case proper nouns conservatively: only the word that immediately
    # follows "вул./пров./пл./просп./бул./наб./м./с./смт".
    def _tc_after_prefix(match: re.Match[str]) -> str:
        prefix = match.group(1)
        word = match.group(2)
        # Don't touch all-uppercase acronyms (ОУН-УПА).
        if word.isupper() and len(word) >= 3:
            return f"{prefix}{word}"
        parts = re.split(r"([-'ʼ’])", word)
        out = []
        for p in parts:
            if p in ("-", "'", "ʼ", "’"):
                out.append(p)
            elif p:
                out.append(p[:1].upper() + p[1:].lower())
        return f"{prefix}{''.join(out)}"

    s = re.sub(
        r"(вул\. |пров\. |просп\. |пл\. |бул\. |наб\. |м\. |с\. |смт )([А-ЯІЇЄҐа-яіїєґA-Za-z][А-ЯІЇЄҐа-яіїєґ\-'ʼ’A-Za-z]*)",
        _tc_after_prefix,
        s,
    )

    s = _collapse_repeats(s)
    # Drop orphan prefixes without arguments: "вул. Х, буд. 5, кв." → drop final "кв."
    s = re.sub(r",\s*(вул\.|буд\.|кв\.|корп\.|пров\.|просп\.|пл\.)\s*$", "", s, flags=re.I)
    s = s.strip(" ,.;")
    return s


# ---------------------------------------------------------------------------
# Tax ID
# ---------------------------------------------------------------------------


def normalize_tax_id(val: Any) -> str:
    """Keep digits only. 8 digits = ЄДРПОУ, 10 digits = РНОКПП."""
    if val is None or val == "":
        return ""
    digits = re.sub(r"\D", "", str(val))
    if len(digits) < 6 or len(digits) > 12:
        return ""
    return digits


# ---------------------------------------------------------------------------
# Date / number
# ---------------------------------------------------------------------------


def normalize_date(val: Any) -> date | None:
    if val is None or val == "":
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    s = str(val).strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def normalize_float(val: Any) -> float | None:
    if val is None or val == "":
        return None
    try:
        return float(str(val).replace(",", ".").replace(" ", ""))
    except (TypeError, ValueError):
        return None


def normalize_text(val: Any) -> str:
    """Generic cleanup — use for cadastral number, purpose, object type, etc."""
    s = _base_clean(val)
    return fix_latin_contamination(s) if s else ""
