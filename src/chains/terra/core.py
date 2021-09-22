from __future__ import annotations

from terra_sdk.core import Coin, Dec, Numeric

from .client import TerraClient


class DecimalizeMixin:
    decimals: int

    def decimalize(self, amount: Numeric.Input) -> Dec:
        return amount / (Dec.one() * 10 ** self.decimals)


class NativeToken(DecimalizeMixin):
    decimals = 6

    def __init__(self, denom: str):
        self.denom = denom
        self.symbol = denom[1:].upper()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.symbol})'

    def __eq__(self, other) -> bool:
        if not isinstance(other, type(self)):
            return False
        return self.denom == other.denom

    def to_msg(self) -> dict:
        return {'native_token': {'denom': self.denom}}


class CW20Token(DecimalizeMixin):
    def __init__(self, contract_addr: str, symbol: str, decimals: int):
        self.contract_addr = contract_addr
        self.symbol = symbol
        self.decimals = decimals

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.symbol})'

    def __eq__(self, other) -> bool:
        if not isinstance(other, type(self)):
            return False
        return self.contract_addr == other.contract_addr

    @classmethod
    def from_contract(cls, contract_addr: str, client: TerraClient) -> CW20Token:
        msg = client.contract_query(contract_addr, {'token_info': {}})
        return cls(contract_addr, msg['symbol'], msg['decimals'])

    def to_msg(self) -> dict:
        return {'token': {'contract_addr': self.contract_addr}}


class TokenAmount:
    def __init__(
        self,
        token: NativeToken | CW20Token,
        amount: Numeric.Input,
        decimalize: bool = False,
    ):
        if decimalize:
            amount = token.decimalize(amount)

        self.token = token
        self.amount = Dec(amount)

    @classmethod
    def from_coin(cls, coin: Coin) -> TokenAmount:
        token = NativeToken(coin.denom)
        amount = Dec(coin.amount)
        return cls(token, amount)

    def update_amount(self, amount: Numeric.Input, decimalize: bool = False):
        if decimalize:
            amount = self.token.decimalize(amount)
        self.amount = Dec(amount)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.token}, {self.amount.to_short_str()})'

    def __eq__(self, other) -> bool:
        assert isinstance(other, type(self)) and self.token == other.token
        return self.amount == other.amount

    def __lt__(self, other) -> bool:
        assert isinstance(other, type(self)) and self.token == other.token
        return self.amount < other.amount

    def __le__(self, other) -> bool:
        assert isinstance(other, type(self)) and self.token == other.token
        return self.amount <= other.amount

    def __gt__(self, other) -> bool:
        assert isinstance(other, type(self)) and self.token == other.token
        return self.amount > other.amount

    def __ge__(self, other) -> bool:
        assert isinstance(other, type(self)) and self.token == other.token
        return self.amount >= other.amount

    def __add__(self, other) -> TokenAmount:
        assert isinstance(other, type(self)) and self.token == other.token
        return TokenAmount(self.token, self.amount + other.amount)

    def __sub__(self, other) -> TokenAmount:
        assert isinstance(other, type(self)) and self.token == other.token
        return TokenAmount(self.token, self.amount - other.amount)

    def __mul__(self, other: Numeric.Input) -> TokenAmount:
        return TokenAmount(self.token, self.amount * Dec(other))

    def __truediv__(self, other: Numeric.Input) -> TokenAmount:
        return TokenAmount(self.token, self.amount / Dec(other))

    def __neg__(self) -> TokenAmount:
        return TokenAmount(self.token, -self.amount)

    def __abs__(self) -> TokenAmount:
        return TokenAmount(self.token, abs(self.amount))

    def __pos__(self) -> TokenAmount:
        return TokenAmount(self.token, +self.amount)
