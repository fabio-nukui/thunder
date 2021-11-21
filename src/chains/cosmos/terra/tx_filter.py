from __future__ import annotations

import base64
import json
import logging
from abc import ABC, abstractmethod
from typing import Iterable

from terra_sdk.core import AccAddress

from . import terraswap
from .native_liquidity_pair import NativeLiquidityPair
from .token import TerraCW20Token, TerraNativeToken, TerraToken

log = logging.getLogger(__name__)


def _decode_msg(raw_msg: str | dict, always_base64: bool) -> dict:
    if isinstance(raw_msg, dict):
        return {} if always_base64 else raw_msg
    return json.loads(base64.b64decode(raw_msg))


def _get_type_value(msg: dict) -> tuple[str, dict]:
    if "_serialized_on_wire" in msg:
        return msg["@type"], msg
    return msg["type"], msg["value"]


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
        if not self.pairs:
            return False

        type_, value = _get_type_value(msgs[0])
        if not type_.endswith("MsgExecuteContract"):
            return False

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


class FilterNativeSwap(Filter):
    def __init__(self, pairs: Iterable[NativeLiquidityPair]):
        self.denoms = [{token.denom for token in pair.tokens} for pair in pairs]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(denoms={self.denoms})"

    def match_msgs(self, msgs: list[dict]) -> bool:
        if not self.denoms:
            return False

        for msg in msgs:
            type_, value = _get_type_value(msg)
            if not type_.endswith("MsgSwap"):
                continue
            for denom_pair in self.denoms:
                if {value["offer_coin"]["denom"], value["ask_denom"]} == denom_pair:
                    return True
        return False


class FilterFirstActionRouterSwap(Filter):
    def __init__(
        self,
        pairs: Iterable[terraswap.RouterLiquidityPair],
        router_addresses: Iterable[AccAddress],
        aways_base64: bool = False,
    ):
        self.aways_base64 = aways_base64
        self.pairs = pairs
        self.router_addresses = router_addresses
        self._pair_ids = [
            (
                "native" if isinstance(p, terraswap.RouterNativeLiquidityPair) else "terraswap",
                {_get_token_id(p.tokens[0]), _get_token_id(p.tokens[1])},
            )
            for p in self.pairs
        ]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(pairs={self.pairs})"

    def match_msgs(self, msgs: list[dict]) -> bool:
        if not self.pairs or not self.router_addresses:
            return False

        type_, value = _get_type_value(msgs[0])
        if not type_.endswith("MsgExecuteContract"):
            return False

        action = "execute_swap_operations"
        operations: list[dict[str, dict]]
        if (
            value["contract"] in self.router_addresses
            and action in (execute_msg := value["execute_msg"])
            and "operations" in (swap_operations := execute_msg[action])
        ):
            operations = swap_operations["operations"]
        elif (
            "send" in (execute_msg := value["execute_msg"])
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
                    native_swap: dict[str, str] = operation["native_swap"]
                    pair_id = ("native", {native_swap["ask_denom"], native_swap["offer_denom"]})
                else:
                    pair_swap: dict[str, dict[str, dict[str, str]]] = operation["terra_swap"]
                    (ask_asset,) = pair_swap["ask_asset_info"].values()
                    (offer_asset,) = pair_swap["offer_asset_info"].values()
                    (ask_asset_id,) = ask_asset.values()
                    (offer_asset_id,) = offer_asset.values()
                    pair_id = ("terraswap", {ask_asset_id, offer_asset_id})
                if any(pair_id == ids for ids in self._pair_ids):
                    return True
        except (KeyError, AttributeError, ValueError):
            log.debug("Unexpected msg format", extra={"data": msgs[0]})
        return False


class FilterSwapTerraswap(Filter):
    def __init__(
        self,
        pairs: Iterable[terraswap.RouterLiquidityPair],
        router_addresses: Iterable[AccAddress],
    ):
        self.pairs = pairs

        filter_length = FilterMsgsLength(1)
        terraswap_pairs = [p for p in self.pairs if isinstance(p, terraswap.LiquidityPair)]
        filter_pair = FilterFirstActionPairSwap(terraswap.Action.swap, terraswap_pairs)
        filter_router = FilterFirstActionRouterSwap(self.pairs, router_addresses)

        self._filter = filter_length & (filter_pair | filter_router)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(pairs={self.pairs})"

    def match_msgs(self, msgs: list[dict]) -> bool:
        return self._filter.match_msgs(msgs)


def _get_token_id(token: TerraToken) -> str:
    if isinstance(token, TerraCW20Token):
        return token.contract_addr
    return token.denom
