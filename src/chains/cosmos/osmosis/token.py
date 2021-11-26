from __future__ import annotations

import json
import re
from functools import cache
from typing import TYPE_CHECKING, Union

from osmosis_proto.cosmos.base.v1beta1 import Coin

from common.token import Token

from ..token import CosmosNativeToken, CosmosTokenAmount, CW20Token

if TYPE_CHECKING:
    from .client import OsmosisClient

_IBC_TOKENS_FILE = "resources/contracts/cosmos/{chain_id}/ibc_tokens.json"
_pat_coin = re.compile(r"^(\-?[0-9]+(\.[0-9]+)?)([0-9a-zA-Z/]+)$")


@cache
def _get_ibc_tokens(chain_id: str) -> list[dict]:
    with open(_IBC_TOKENS_FILE.format(chain_id=chain_id)) as f:
        return json.load(f)


@cache
def _get_ibc_symbol(denom: str, chain_id: str) -> str:
    tokens = _get_ibc_tokens(chain_id)
    denom_hash = denom.partition("/")[2]
    ((base_denom, path),) = [
        (t["base_denom"], t["path"]) for t in tokens if t["denom_hash"] == denom_hash
    ]
    channels = path.replace("transfer/", "")
    if base_denom[0] == "u":
        base_denom = base_denom[1:]
    symbol = f"{base_denom.upper()}(ibc/{channels})"
    return symbol


class OsmosisTokenAmount(CosmosTokenAmount):
    token: OsmosisToken

    @classmethod
    def from_coin(cls, coin: Coin, client: OsmosisClient = None) -> OsmosisTokenAmount:
        token = OsmosisNativeToken(coin.denom, client)
        return cls(token, int_amount=coin.amount)

    def to_coin(self) -> Coin:
        assert isinstance(self.token, OsmosisNativeToken)
        return Coin(self.token.denom, str(self.int_amount))

    @classmethod
    def from_str(cls, string: str) -> OsmosisTokenAmount:
        if not (match := _pat_coin.match(string)):
            raise ValueError(f"failed to parse Coin: {string}")
        return OsmosisNativeToken(match.group(3)).to_amount(int_amount=match.group(1))

    def to_str(self) -> str:
        if isinstance(self.token, OsmosisNativeToken):
            return f"{self.int_amount}{self.token.denom}"
        raise NotImplementedError


class BaseOsmosisToken(Token[OsmosisTokenAmount]):
    amount_class = OsmosisTokenAmount


class OsmosisNativeToken(BaseOsmosisToken, CosmosNativeToken[OsmosisTokenAmount]):
    def __init__(self, denom: str, client: OsmosisClient = None):
        if not denom.startswith("ibc/"):
            return super().__init__(denom, decimals=6)
        if client is None:
            return super().__init__(denom, decimals=6, symbol=denom)
        symbol = _get_ibc_symbol(denom, client.chain_id)
        super().__init__(denom, decimals=6, symbol=symbol)


class OsmosisCW20Token(BaseOsmosisToken, CW20Token[OsmosisTokenAmount]):
    pass


OsmosisToken = Union[OsmosisNativeToken, OsmosisCW20Token]
