"""Checksum validation for ЄДРПОУ (8-digit legal-entity ID) and РНОКПП
(10-digit individual tax ID).

Neither algorithm is secret: both are published by ДПС and standard across
integrators. Catching a bad checksum means the number is almost certainly a
data-entry typo rather than a real entity. We use it as a LOW-severity
signal because a handful of historical IDs legitimately fail the algorithm
(legacy pre-1997 entries, some state bodies).
"""

from __future__ import annotations


def edrpou_checksum_valid(edrpou: str | int | None) -> bool:
    """ЄДРПОУ is an 8-digit identifier; the 8th digit is a control digit
    over the first 7. Algorithm:
      1. s1 = Σ d[i] × (i + 1) for i in 0..6
      2. If s1 % 11 < 10: control = s1 % 11
      3. Else: s2 = Σ d[i] × (i + 3) for i in 0..6
         If s2 % 11 < 10: control = s2 % 11
         Else: control = 0
      4. Valid iff control == d[7]
    """
    if edrpou is None:
        return True  # nothing to check
    s = str(edrpou).strip()
    if len(s) != 8 or not s.isdigit():
        return False
    digits = [int(c) for c in s]
    s1 = sum(d * (i + 1) for i, d in enumerate(digits[:7]))
    mod1 = s1 % 11
    if mod1 < 10:
        control = mod1
    else:
        s2 = sum(d * (i + 3) for i, d in enumerate(digits[:7]))
        mod2 = s2 % 11
        control = mod2 if mod2 < 10 else 0
    return control == digits[7]


def rnokpp_checksum_valid(rnokpp: str | int | None) -> bool:
    """РНОКПП (Реєстраційний номер облікової картки платника податків) —
    10-digit individual tax ID. The 10th digit is a control digit.
    Algorithm:
      1. weights = [-1, 5, 7, 9, 4, 6, 10, 5, 7]
      2. s = Σ d[i] × weights[i] for i in 0..8
      3. control = (s % 11) % 10
      4. Valid iff control == d[9]
    """
    if rnokpp is None:
        return True
    s = str(rnokpp).strip()
    if len(s) != 10 or not s.isdigit():
        return False
    digits = [int(c) for c in s]
    weights = [-1, 5, 7, 9, 4, 6, 10, 5, 7]
    total = sum(d * w for d, w in zip(digits[:9], weights))
    control = (total % 11) % 10
    return control == digits[9]
