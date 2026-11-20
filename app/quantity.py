"""Minimal Kubernetes resource.Quantity parser.

Only the shapes that appear in PersistentVolume capacity / PVC storage
requests are supported: plain numbers, decimal SI suffixes and binary SI
suffixes, plus an optional scientific exponent.

The parsed value is returned as a ``Decimal`` expressed in base units
(bytes for storage), which is precise enough to compare storage capacities
without floating point rounding surprises.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation


class QuantityError(ValueError):
    """Raised when a quantity string cannot be parsed."""


_BINARY = {
    "Ki": Decimal(1024),
    "Mi": Decimal(1024) ** 2,
    "Gi": Decimal(1024) ** 3,
    "Ti": Decimal(1024) ** 4,
    "Pi": Decimal(1024) ** 5,
    "Ei": Decimal(1024) ** 6,
}

_DECIMAL = {
    "n": Decimal("1e-9"),
    "u": Decimal("1e-6"),
    "m": Decimal("1e-3"),
    "": Decimal(1),
    "k": Decimal(1000),
    "K": Decimal(1000),
    "M": Decimal(1000) ** 2,
    "G": Decimal(1000) ** 3,
    "T": Decimal(1000) ** 4,
    "P": Decimal(1000) ** 5,
    "E": Decimal(1000) ** 6,
}

_PATTERN = re.compile(
    r"^\s*(?P<number>[+-]?(?:\d+(?:\.\d*)?|\.\d+))"
    r"(?P<exponent>[eE][+-]?\d+)?"
    r"(?P<suffix>[A-Za-z]{0,2})\s*$"
)


def parse_quantity(value: str | int | float | Decimal) -> Decimal:
    """Parse a Kubernetes quantity into a base-unit Decimal."""
    if isinstance(value, bool):  # bool is an int subclass; reject it explicitly
        raise QuantityError(f"invalid quantity: {value!r}")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    if not isinstance(value, str):
        raise QuantityError(f"invalid quantity: {value!r}")

    match = _PATTERN.match(value)
    if not match:
        raise QuantityError(f"invalid quantity: {value!r}")

    try:
        number = Decimal(match.group("number"))
        if match.group("exponent"):
            number *= Decimal("1" + match.group("exponent"))
    except InvalidOperation as exc:  # pragma: no cover - regex already guards this
        raise QuantityError(f"invalid quantity: {value!r}") from exc

    suffix = match.group("suffix")
    if suffix in _BINARY:
        multiplier = _BINARY[suffix]
    elif suffix in _DECIMAL:
        multiplier = _DECIMAL[suffix]
    else:
        raise QuantityError(f"unsupported quantity suffix: {value!r}")

    return number * multiplier
