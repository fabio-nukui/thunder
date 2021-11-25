from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import ROUND_DOWN, Decimal, getcontext
from typing import Generic, Optional, TypeVar, Union, overload

DecInput = Union[str, int, float, Decimal]
ROUNDING_SAFETY_MARGIN = 10

getcontext().prec = 78  # To allow for calculations with up to 256 bits precision
getcontext().rounding = ROUND_DOWN

_MAX_DECIMALS_REPR = 8
_MAX_DECIMALS_DATA = 18

_TokenAmountT = TypeVar("_TokenAmountT", bound="TokenAmount")


class Token(Generic[_TokenAmountT], ABC):
    symbol: str
    decimals: int
    amount_class: type[_TokenAmountT]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.repr_symbol})"

    def __str__(self) -> str:
        return self.repr_symbol

    def to_amount(
        self,
        amount: DecInput = None,
        int_amount: Optional[int | str] = None,
    ) -> _TokenAmountT:
        return self.amount_class(self, amount, int_amount)

    @property
    @abstractmethod
    def _id(self) -> tuple:
        ...

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


class TokenAmount:
    def __init__(
        self,
        token: Token,
        amount: DecInput = None,
        int_amount: Optional[int | str] = None,
    ):
        self.token = token
        self.dx = Decimal(str(10 ** -self.decimals))

        self.amount = Decimal("NaN")

        if amount is not None:
            self.amount = Decimal(amount)
        elif int_amount is not None:
            self.set_int_amount(int_amount)

    def __repr__(self) -> str:
        decimals = min(_MAX_DECIMALS_REPR, self.decimals)
        return f"{self.__class__.__name__}({self.token}: {self.amount:,.{decimals}f})"

    def to_data(self) -> dict:
        return {
            "symbol": self.symbol,
            "amount": str(round(self.amount, _MAX_DECIMALS_DATA)),
        }

    @property
    def symbol(self) -> str:
        return self.token.symbol

    @property
    def decimals(self) -> int:
        return self.token.decimals

    @property
    def int_amount(self) -> int:
        assert not self.is_empty()
        return int(self.amount * 10 ** self.decimals)

    def set_int_amount(self, value: int | str):
        self.amount = self.token.decimalize(value)

    def safe_margin(self: _TokenAmountT, margin: bool | int = True) -> _TokenAmountT:
        if margin is False:
            return self
        margin = ROUNDING_SAFETY_MARGIN if margin is True else margin
        return self.token.to_amount(int_amount=self.int_amount - margin)

    def is_empty(self) -> bool:
        return self.amount.is_nan()

    def _to_decimal(self, value) -> Decimal:
        if isinstance(value, type(self)):
            assert self.token == value.token, "Operation only allowed for identical tokens"
            return value.amount
        try:
            return Decimal(value)
        except TypeError:
            return NotImplemented

    def __eq__(self, other) -> bool:
        return self.amount == self._to_decimal(other)

    def __ne__(self, other) -> bool:
        return self.amount != self._to_decimal(other)

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

    def __radd__(self: _TokenAmountT, other: _TokenAmountT | DecInput) -> _TokenAmountT:
        return self.__add__(other)

    def __sub__(self: _TokenAmountT, other: _TokenAmountT | DecInput) -> _TokenAmountT:
        return self.__class__(self.token, self.amount - self._to_decimal(other))

    def __mul__(self: _TokenAmountT, other: DecInput) -> _TokenAmountT:
        return self.__class__(self.token, self.amount * Decimal(other))

    def __rmul__(self: _TokenAmountT, other: DecInput) -> _TokenAmountT:
        return self.__mul__(other)

    @overload
    def __truediv__(self: _TokenAmountT, other: DecInput) -> _TokenAmountT:
        ...

    @overload
    def __truediv__(self: _TokenAmountT, other: _TokenAmountT) -> Decimal:
        ...

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

    def __bool__(self) -> bool:
        return bool(self.amount)
