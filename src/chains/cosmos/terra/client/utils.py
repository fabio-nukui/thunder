from __future__ import annotations

from decimal import Decimal

_PROTO_PRECISION = 10 ** 18


def parse_proto_decimal(x: str | bytes) -> Decimal:
    if isinstance(x, bytes):
        x = x.decode("ascii")
    return Decimal(x) / _PROTO_PRECISION
