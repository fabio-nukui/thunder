from __future__ import annotations

import json
from functools import cache
from typing import TYPE_CHECKING, Union

from terra_sdk.core import Coin

from common.token import Token

from ..token import CosmosNativeToken, CosmosTokenAmount, CW20Token

if TYPE_CHECKING:
    from .client import OsmosisClient

_IBC_TOKENS_FILE = "resources/contracts/cosmos/{chain_id}/ibc_tokens.json"


@cache
def _get_ibc_tokens(chain_id: str) -> list[dict]:
    with open(_IBC_TOKENS_FILE.format(chain_id=chain_id)) as f:
        return json.load(f)


class OsmosisTokenAmount(CosmosTokenAmount):
    token: OsmosisToken

    @classmethod
    def from_coin(cls, coin: Coin, client: OsmosisClient = None) -> OsmosisTokenAmount:
        token = OsmosisNativeToken(coin.denom, client)
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
    def __init__(self, denom: str, client: OsmosisClient = None):
        if not denom.startswith("ibc/"):
            return super().__init__(denom, decimals=6)
        if client is None:
            return super().__init__(denom, decimals=6, symbol=denom)
        tokens = _get_ibc_tokens(client.chain_id)
        denom_hash = denom.partition("/")[2]
        ((base_denom, path),) = [
            (t["base_denom"], t["path"]) for t in tokens if t["denom_hash"] == denom_hash
        ]
        channels = path.replace("transfer/", "")
        symbol = f"{base_denom[1:].upper()}(ibc/{channels})"
        super().__init__(denom, decimals=6, symbol=symbol)


class OsmosisCW20Token(BaseOsmosisToken, CW20Token[OsmosisTokenAmount]):
    pass


OsmosisToken = Union[OsmosisNativeToken, OsmosisCW20Token]
