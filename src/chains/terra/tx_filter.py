from __future__ import annotations

import base64
import json
from abc import ABC, abstractmethod
from typing import Iterable

from . import terraswap
from .token import TerraNativeToken


def _decode_msg(raw_msg: str | dict, always_base64: bool = True) -> dict:
    if isinstance(raw_msg, dict):
        return {} if always_base64 else raw_msg
    return json.loads(base64.b64decode(raw_msg))


class Filter(ABC):
    @abstractmethod
    def match_msgs(self, msgs: list[dict]) -> bool:
        ...

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
    def __init__(
        self,
        action: terraswap.Action,
        pairs: Iterable[terraswap.LiquidityPair],
        aways_base64: bool = True,
    ):
        self.action = action
        self.pairs = list(pairs)
        self.aways_base64 = aways_base64

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(action={self.action}, pairs={self.pairs})"

    def match_msgs(self, msgs: list[dict]) -> bool:
        msg = msgs[0]
        if "MsgExecuteContract" not in msg["type"]:
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
                    and self.action in _decode_msg(send["msg"], self.aways_base64)
                ):
                    return True
        return False


class FilterSingleSwapTerraswapPair(Filter):
    def __init__(self, pair: terraswap.LiquidityPair):
        self.pair = pair
        terraswap_filter = FilterFirstActionTerraswap(terraswap.Action.swap, [self.pair])
        self._filter = FilterMsgsLength(1) & terraswap_filter

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.pair})"

    def match_msgs(self, msgs: list[dict]) -> bool:
        return self._filter.match_msgs(msgs)
