from __future__ import annotations

from decimal import Decimal, getcontext
from typing import Optional, TypeVar, Union, overload

DecInput = Union[str, int, float, Decimal]
DEFAULT_DECIMALS = 18
ROUNDING_SAFETY_MARGIN = 2

getcontext().prec = 78  # To allow for calculations with up to 256 bits precision

_MAX_DECIMALS_REPR = 8


class Token:
    symbol: str
    decimals: int = DEFAULT_DECIMALS

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.repr_symbol})'

    def __str__(self) -> str:
        return self.repr_symbol

    @property
    def _id(self) -> tuple:
        raise NotImplementedError

    @property
    def repr_symbol(self):
        return self.symbol

    def __hash__(self) -> int:
        return hash(self._id)

    def __eq__(self, other) -> bool:
        if isinstance(other, type(self)):
            return self._id == other._id
        return NotImplemented

    def decimalize(self, value: DecInput) -> Decimal:
        return Decimal(value) / 10 ** self.decimals

    def round(self, value: DecInput) -> Decimal:
        return round(Decimal(value), self.decimals)


_TokenAmountT = TypeVar('_TokenAmountT', bound='TokenAmount')


class TokenAmount:
    def __init__(
        self,
        token: Token,
        amount: DecInput = Decimal('NaN'),
        int_amount: Optional[int | str] = None,
    ):
        self.token = token
        self.symbol = self.token.symbol
        self.decimals = self.token.decimals
        self.dx = Decimal(str(10 ** -self.decimals))

        self._amount: Decimal = Decimal('NaN')

        amount = Decimal(amount)
        if not amount.is_nan():
            self.amount = amount
        elif int_amount is not None:
            self.int_amount = int_amount

    def __repr__(self) -> str:
        decimals = min(_MAX_DECIMALS_REPR, self.decimals)
        return f'{self.__class__.__name__}({self.token}: {self.amount:,.{decimals}f})'

    def to_data(self) -> dict:
        return {
            'symbol': self.symbol,
            'amount': str(self.amount),
        }

    @property
    def amount(self) -> Decimal:
        return self._amount

    @amount.setter
    def amount(self, value: DecInput):  # type: ignore
        self._amount = Decimal(value)

    @property
    def round_amount(self) -> Decimal:
        return self.token.round(self._amount)

    @property
    def int_amount(self) -> int:
        assert not self.is_empty()
        return int(self.amount * 10 ** self.decimals)

    @int_amount.setter
    def int_amount(self, value: int | str):  # type: ignore
        self._amount = self.token.decimalize(value)

    def safe_down(self: _TokenAmountT) -> _TokenAmountT:
        return self - self.dx * ROUNDING_SAFETY_MARGIN

    def safe_up(self: _TokenAmountT) -> _TokenAmountT:
        return self + self.dx * ROUNDING_SAFETY_MARGIN

    def is_empty(self) -> bool:
        return self.amount.is_nan()

    def _to_decimal(self, value) -> Decimal:
        if isinstance(value, type(self)):
            assert self.token == value.token
            return value.amount
        try:
            return Decimal(value)
        except TypeError:
            return NotImplemented

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

    def __add__(self: _TokenAmountT, other: _TokenAmountT | DecInput) -> _TokenAmountT:
        return self.__class__(self.token, self.amount + self._to_decimal(other))

    def __sub__(self: _TokenAmountT, other: _TokenAmountT | DecInput) -> _TokenAmountT:
        return self.__class__(self.token, self.amount - self._to_decimal(other))

    def __mul__(self: _TokenAmountT, other: DecInput) -> _TokenAmountT:
        return self.__class__(self.token, self.amount * Decimal(other))

    @overload
    def __truediv__(self: _TokenAmountT, other: DecInput) -> _TokenAmountT: ...  # noqa: E704

    @overload
    def __truediv__(self: _TokenAmountT, other: _TokenAmountT) -> Decimal: ...  # noqa: E704

    def __truediv__(self, other):
        result = self.amount / self._to_decimal(other)
        if isinstance(other, type(self)):
            return result
        return self.__class__(self.token, result)

    def __neg__(self: _TokenAmountT) -> _TokenAmountT:
        return self.__class__(self.token, -self.amount)

    def __abs__(self: _TokenAmountT) -> _TokenAmountT:
        return self.__class__(self.token, abs(self.amount))

    def __pos__(self: _TokenAmountT) -> _TokenAmountT:
        return self.__class__(self.token, +self.amount)
