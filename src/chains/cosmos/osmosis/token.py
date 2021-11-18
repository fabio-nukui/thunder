from __future__ import annotations

from typing import Union

from terra_sdk.core import Coin

from common.token import Token

from ..token import CosmosNativeToken, CosmosTokenAmount, CW20Token


class OsmosisTokenAmount(CosmosTokenAmount):
    token: OsmosisToken

    @classmethod
    def from_coin(cls, coin: Coin) -> OsmosisTokenAmount:
        token = OsmosisNativeToken(coin.denom)
        return cls(token, int_amount=coin.amount)

    def to_coin(self) -> Coin:
        assert isinstance(self.token, OsmosisNativeToken)
        return Coin(self.token.denom, self.int_amount)

    @classmethod
    def from_str(cls, data: str) -> OsmosisTokenAmount:
        return cls.from_coin(Coin.from_str(data))


class BaseOsmosisToken(Token[OsmosisTokenAmount]):
    amount_class = OsmosisTokenAmount


class OsmosisNativeToken(BaseOsmosisToken, CosmosNativeToken[OsmosisTokenAmount]):
    def __init__(self, denom: str):
        super().__init__(denom, decimals=6)


class OsmosisCW20Token(BaseOsmosisToken, CW20Token[OsmosisTokenAmount]):
    pass


OsmosisToken = Union[OsmosisNativeToken, OsmosisCW20Token]
