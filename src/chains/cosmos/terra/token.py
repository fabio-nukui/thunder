from __future__ import annotations

from typing import Union

from terra_sdk.core import Coin

from common.token import Token

from ..token import CosmosNativeToken, CosmosTokenAmount, CW20Token


class TerraTokenAmount(CosmosTokenAmount):
    token: TerraToken

    @classmethod
    def from_coin(cls, coin: Coin) -> TerraTokenAmount:
        token = TerraNativeToken(coin.denom)
        return cls(token, int_amount=coin.amount)

    def to_coin(self) -> Coin:
        assert isinstance(self.token, TerraNativeToken)
        return Coin(self.token.denom, self.int_amount)

    @classmethod
    def from_str(cls, data: str) -> TerraTokenAmount:
        return cls.from_coin(Coin.from_str(data))


class BaseTerraToken(Token[TerraTokenAmount]):
    amount_class = TerraTokenAmount


class TerraNativeToken(BaseTerraToken, CosmosNativeToken[TerraTokenAmount]):
    def __init__(self, denom: str):
        symbol = "LUNA" if denom == "uluna" else denom[1:-1].upper() + "T"
        if denom[0] == "u":
            decimals = 6
        else:
            raise NotImplementedError("TerraNativeToken only implemented for micro (µ) demons")
        super().__init__(denom, decimals, symbol)


class TerraCW20Token(BaseTerraToken, CW20Token[TerraTokenAmount]):
    pass


TerraToken = Union[TerraNativeToken, TerraCW20Token]
