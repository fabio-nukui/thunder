from __future__ import annotations

import base64
import json
from enum import Enum
from typing import Iterable

from .interfaces import IFilter
from .terraswap import LiquidityPair
from .token import TerraNativeToken


class TerraswapAction(str, Enum):
    swap = "swap"
    remove_liquidity = "remove_liquidity"
    add_liquidity = "add_liquidity"


def _decode_msg(raw_msg: str) -> dict:
    return json.loads(base64.b64decode(raw_msg))


class Filter(IFilter):
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}"

    def __and__(self: Filter, other) -> FilterAll:
        if not isinstance(other, Filter):
            return NotImplemented
        self_filters = self.filters if isinstance(self, FilterAll) else [self]
        other_filters = other.filters if isinstance(other, FilterAll) else [other]
        return FilterAll(self_filters + other_filters)

    def __or__(self: Filter, other) -> FilterAny:
        if not isinstance(other, Filter):
            return NotImplemented
        self_filters = self.filters if isinstance(self, FilterAny) else [self]
        other_filters = other.filters if isinstance(other, FilterAny) else [other]
        return FilterAny(self_filters + other_filters)


class FilterAll(Filter):
    def __init__(self, filters: list[Filter]):
        self.filters = filters

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.filters})"

    def match_msgs(self, msgs: list[dict]) -> bool:
        return all(filter_.match_msgs(msgs) for filter_ in self.filters)


class FilterAny(Filter):
    def __init__(self, filters: list[Filter]):
        self.filters = filters

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.filters})"

    def match_msgs(self, msgs: list[dict]) -> bool:
        return any(filter_.match_msgs(msgs) for filter_ in self.filters)


class FilterMsgsLength(Filter):
    def __init__(self, length: int):
        self.length = length

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(length={self.length})"

    def match_msgs(self, msgs: list[dict]) -> bool:
        return len(msgs) == self.length


class FilterFirstActionTerraswap(Filter):
    def __init__(self, action: TerraswapAction, pairs: Iterable[LiquidityPair]):
        self.action = action
        self.pairs = list(pairs)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(action={self.action}, pairs={self.pairs})"

    def match_msgs(self, msgs: list[dict]) -> bool:
        msg = msgs[0]
        if msg["type"] != "wasm/MsgExecuteContract":
            return False
        value = msg["value"]

        for pair in self.pairs:
            for token in pair.tokens:
                if isinstance(token, TerraNativeToken):
                    if (
                        value["contract"] == pair.contract_addr
                        and self.action in value["execute_msg"]
                    ):
                        return True
                elif (
                    value["contract"] == token.contract_addr
                    and "send" in (execute_msg := value["execute_msg"])
                    and "msg" in (send := execute_msg["send"])
                    and send["contract"] == pair.contract_addr
                    and self.action in _decode_msg(send["msg"])
                ):
                    return True
        return False


class FilterSingleSwapTerraswapPair(Filter):
    def __init__(self, pair: LiquidityPair):
        self.pair = pair
        terraswap_filter = FilterFirstActionTerraswap(TerraswapAction.swap, [self.pair])
        self._filter = FilterMsgsLength(1) & terraswap_filter

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.pair})"

    def match_msgs(self, msgs: list[dict]) -> bool:
        return self._filter.match_msgs(msgs)
