from __future__ import annotations

from decimal import Decimal, getcontext
from typing import Optional, Union

DecInput = Union[str, int, float, Decimal]
DEFAULT_DECIMALS = 18

getcontext().prec = 78  # To allow for calculations with up to 256 bits precision

_MAX_DECIMALS_REPR = 8


class Token:
    symbol: str
    decimals: int = DEFAULT_DECIMALS

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.symbol})'

    def __str__(self) -> str:
        return self.symbol

    @property
    def _id(self) -> tuple:
        return (self.symbol, self.decimals)

    def __eq__(self, other) -> bool:
        return isinstance(other, type(self)) and self._id == other._id

    def decimalize(self, value: DecInput) -> Decimal:
        return Decimal(value) / 10 ** self.decimals

    def round(self, value: DecInput) -> Decimal:
        return round(Decimal(value), self.decimals)


class TokenAmount:
    def __init__(
        self,
        token: Token,
        amount: DecInput = Decimal('NaN'),
        raw_amount: Optional[int | str] = None,
    ):
        self.token = token
        self.symbol = self.token.symbol
        self.decimals = self.token.decimals

        self._amount: Decimal = Decimal('NaN')

        amount = Decimal(amount)
        if not amount.is_nan():
            self.amount = amount
        elif raw_amount is not None:
            self.raw_amount = raw_amount

    def __repr__(self) -> str:
        decimals = min(_MAX_DECIMALS_REPR, self.decimals)
        return f'{self.__class__.__name__}({self.token}: {self.amount:,.{decimals}f})'

    @property
    def amount(self) -> Decimal:
        return self._amount

    @amount.setter
    def amount(self, value: DecInput):  # type: ignore
        self._amount = self.token.round(value)

    @property
    def raw_amount(self) -> int:
        assert not self.is_empty()
        return int(self.amount * 10 ** self.decimals)

    @raw_amount.setter
    def raw_amount(self, value: int | str):  # type: ignore
        self._amount = self.token.decimalize(value)

    def is_empty(self) -> bool:
        return self.amount.is_nan()

    def _to_decimal(self, value) -> Decimal:
        if isinstance(value, type(self)):
            assert self.token == value.token
            return value.amount
        return Decimal(value)

    def __eq__(self, other) -> bool:
        return self.amount == self._to_decimal(other)

    def __lt__(self, other) -> bool:
        return self.amount < self._to_decimal(other)

    def __le__(self, other) -> bool:
        return self.amount <= self._to_decimal(other)

    def __gt__(self, other) -> bool:
        return self.amount > self._to_decimal(other)

    def __ge__(self, other) -> bool:
        return self.amount >= self._to_decimal(other)

    def __add__(self, other: TokenAmount | DecInput) -> TokenAmount:
        return TokenAmount(self.token, self.amount + self._to_decimal(other))

    def __sub__(self, other: TokenAmount | DecInput) -> TokenAmount:
        return TokenAmount(self.token, self.amount - self._to_decimal(other))

    def __mul__(self, other: DecInput) -> TokenAmount:
        return TokenAmount(self.token, self.amount * Decimal(other))

    def __truediv__(self, other: DecInput) -> TokenAmount:
        return TokenAmount(self.token, self.amount / Decimal(other))

    def __neg__(self) -> TokenAmount:
        return TokenAmount(self.token, -self.amount)

    def __abs__(self) -> TokenAmount:
        return TokenAmount(self.token, abs(self.amount))

    def __pos__(self) -> TokenAmount:
        return TokenAmount(self.token, +self.amount)
