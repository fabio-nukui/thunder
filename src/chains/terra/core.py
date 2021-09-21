from __future__ import annotations

from terra_sdk.core import Coin, Dec, Numeric

from .client import TerraClient


class CW20Token:
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
        msg = client.lcd.wasm.contract_query(contract_addr, {'token_info': {}})
        return cls(contract_addr, msg['symbol'], msg['decimals'])

    def to_msg(self) -> dict:
        return {'token': {'contract_addr': self.contract_addr}}


class NativeToken:
    def __init__(self, denom: str):
        self.denom = denom

    def to_msg(self) -> dict:
        return {'native_token': {'denom': self.denom}}


class TokenAmount:
    def __init__(self, token: NativeToken | CW20Token, amount: Numeric.Input):
        self.token = token
        self.amount = Dec(amount)

    @classmethod
    def from_coin(cls, coin: Coin) -> TokenAmount:
        token = NativeToken(coin.denom)
        amount = Dec(coin.amount)
        return cls(token, amount)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.amount.to_short_str()})'

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
