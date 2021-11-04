from __future__ import annotations

import base64
import json
import logging
from abc import ABC, abstractmethod
from typing import Iterable

from . import terraswap
from .token import CW20Token, TerraNativeToken, TerraToken

log = logging.getLogger(__name__)


def _decode_msg(raw_msg: str | dict, always_base64: bool) -> dict:
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


class FilterFirstActionPairSwap(Filter):
    def __init__(
        self,
        action: terraswap.Action,
        pairs: Iterable[terraswap.LiquidityPair],
        aways_base64: bool = False,
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


class FilterFirstActionRouterSwap(Filter):
    def __init__(
        self,
        pairs: Iterable[terraswap.LiquidityPair],
        aways_base64: bool = False,
    ):
        self.aways_base64 = aways_base64
        self.pairs = [p for p in pairs if p.router_address]
        self.router_addresses = {p.router_address for p in self.pairs}
        self._token_contracts = {
            token.contract_addr
            for p in self.pairs
            for token in p.tokens
            if isinstance(token, CW20Token)
        }
        self._token_ids = [
            {_get_token_id(p.tokens[0]), _get_token_id(p.tokens[1])} for p in self.pairs
        ]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(pairs={self.pairs})"

    def match_msgs(self, msgs: list[dict]) -> bool:
        if not self.pairs:
            return False
        msg = msgs[0]
        if "MsgExecuteContract" not in msg["type"]:
            return False
        value = msg["value"]

        action = "execute_swap_operations"
        operations: list[dict[str, dict]]
        if (
            value["contract"] in self.router_addresses
            and action in (execute_msg := value["execute_msg"])
            and "operations" in (swap_operations := execute_msg[action])
        ):
            operations = swap_operations["operations"]
        elif (
            value["contract"] in self._token_contracts
            and "send" in (execute_msg := value["execute_msg"])
            and "msg" in (send := execute_msg["send"])
            and send["contract"] in self.router_addresses
            and action in (inner_msg := _decode_msg(send["msg"], self.aways_base64))
            and "operations" in (swap_operations := inner_msg[action])
        ):
            operations = swap_operations["operations"]
        else:
            return False
        try:
            for operation in operations:
                if "native_swap" in operation:
                    operation_ids = {
                        operation["native_swap"]["ask_denom"],
                        operation["native_swap"]["offer_denom"],
                    }
                else:
                    (ask_asset,) = operation["terra_swap"]["ask_asset_info"].values()
                    (offer_asset,) = operation["terra_swap"]["offer_asset_info"].values()
                    (ask_asset_id,) = ask_asset.values()
                    (offer_asset_id,) = offer_asset.values()
                    operation_ids = {ask_asset_id, offer_asset_id}
                if any(operation_ids == ids for ids in self._token_ids):
                    return True
        except (KeyError, AttributeError, ValueError):
            log.debug("Unexpected msg format", extra={"data": msg})
        return False


class FilterSwapTerraswap(Filter):
    def __init__(self, pairs: Iterable[terraswap.LiquidityPair]):
        self.pairs = pairs
        filter_length = FilterMsgsLength(1)
        filter_pair = FilterFirstActionRouterSwap(self.pairs)
        filter_router = FilterFirstActionRouterSwap(self.pairs)
        self._filter = filter_length & (filter_pair | filter_router)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(pairs={self.pairs})"

    def match_msgs(self, msgs: list[dict]) -> bool:
        return self._filter.match_msgs(msgs)


def _get_token_id(token: TerraToken) -> str:
    if isinstance(token, CW20Token):
        return token.contract_addr
    return token.denom
