from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, Sequence, TypedDict, TypeVar

from terra_sdk.core.broadcast import SyncTxBroadcastResult
from terra_sdk.core.fee import Fee

from chains.cosmos.msg import Msg, MsgType
from exceptions import BlockchainNewState, TxAlreadyBroadcasted

from .base_api import Api

if TYPE_CHECKING:
    from .async_client import CosmosClient

log = logging.getLogger(__name__)

_BROADCASTER_CACHE_BLOCKS = 2
_MsgType = TypeVar("_MsgType", dict, list)


class BroadcasterPayload(TypedDict):
    height: int
    msgs: list[dict]
    n_repeat: int
    fee: dict | None
    fee_denom: str | None


class BroadcasterResponse(TypedDict):
    result: Literal["broadcasted"] | Literal["repeated_tx"] | Literal["new_block"]
    data: list[tuple[float, dict]]


class BroadcastCacheKey(NamedTuple):
    msgs: list[dict]
    n_repeat: int


def _msg_to_key(msg: _MsgType) -> _MsgType:
    if isinstance(msg, dict):
        return {k: _round_msg_values(v) for k, v in msg.items() if k != "msg"}  # type: ignore # https://githubmemory.com/repo/microsoft/pyright/issues/2428 # noqa: E501
    return [_round_msg_values(v) for v in msg]  # type: ignore


def _round_msg_values(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return _msg_to_key(value)  # type: ignore # https://githubmemory.com/repo/microsoft/pyright/issues/2428 # noqa: E501
    if isinstance(value, str):
        try:
            return math.floor(math.log10(float(value)))
        except ValueError:
            return value
    if isinstance(value, (int, float)):
        return math.floor(math.log10(value))
    return value


def _get_pools(msgs: list[dict]) -> set[str]:
    signature = set()
    for msg in msgs:
        type_ = msg["@type"] if "@type" in msg else msg["type"]
        if "MsgExecuteContract" not in type_:
            continue
        execute_msg = msg["execute_msg"]
        if "swap" in execute_msg:  # direct swap
            signature.add(msg["contract"])
        if "send" in execute_msg:  # CW20 send swap
            signature.add(execute_msg["send"]["contract"])
        if "execute_swap_operations" in execute_msg:  # router swap
            signature.add(str(execute_msg["execute_swap_operations"]["operations"]))
    return signature


class BroadcasterApi(Api["CosmosClient"]):
    def __init__(self, client: CosmosClient):
        super().__init__(client)
        self._height: int = 0
        self._broadcaster_cache: dict[int, list[BroadcastCacheKey]] = {}
        self._current_pools: set[str] = set()

    async def post(
        self,
        msgs: Sequence[MsgType],
        n_repeat: int,
        fee: Fee = None,
        fee_denom: str = None,
    ) -> list[tuple[float, SyncTxBroadcastResult]]:
        payload: BroadcasterPayload = {
            "height": self.client.height,
            "msgs": [m.to_data() for m in msgs],
            "n_repeat": n_repeat,
            "fee": fee.to_data() if fee is not None else None,
            "fee_denom": fee_denom,
        }
        assert self.client.active_broadcaster is not None
        log.info(f"Posting to broadcaster {self.client.active_broadcaster.base_url}")
        res = await self.client.active_broadcaster.post("txs", json=payload, n_tries=1)
        data: BroadcasterResponse = res.json()
        if data["result"] == "repeated_tx":
            raise TxAlreadyBroadcasted("Tx broadcasted by other host")
        if data["result"] == "new_block":
            raise BlockchainNewState("Broadcaster on newer block")
        return [
            (timestamp, SyncTxBroadcastResult(**result)) for timestamp, result in data["data"]
        ]

    async def broadcast(self, payload: BroadcasterPayload) -> BroadcasterResponse:
        assert not self.client.use_broadcaster
        if payload["height"] > self._height:
            self._height = payload["height"]
            self._current_pools = set()
        elif payload["height"] < self._height:
            return {"result": "new_block", "data": []}

        tx_pools = _get_pools(payload["msgs"])
        if self._current_pools & tx_pools or self._is_repeated_tx(payload):
            return {"result": "repeated_tx", "data": []}
        self._current_pools |= tx_pools

        msgs = [Msg.from_data(d) for d in payload["msgs"]]
        n_repeat = payload["n_repeat"]
        fee = Fee.from_data(payload["fee"]) if payload["fee"] is not None else None
        fee_denom = payload["fee_denom"]

        try:
            res = await self.client.tx.execute_multi_msgs(msgs, n_repeat, fee, fee_denom)
        except TxAlreadyBroadcasted:
            return {"result": "repeated_tx", "data": []}
        except BlockchainNewState:
            return {"result": "new_block", "data": []}
        return {
            "result": "broadcasted",
            "data": [(timestamp, result.to_data()) for timestamp, result in res],
        }

    def _is_repeated_tx(self, payload: BroadcasterPayload) -> bool:
        self._broadcaster_cache = {  # Drop old values
            height: val
            for height, val in self._broadcaster_cache.items()
            if payload["height"] - height <= _BROADCASTER_CACHE_BLOCKS
        }
        msg_keys = [_msg_to_key(msg) for msg in payload["msgs"]]
        key = BroadcastCacheKey(msg_keys, payload["n_repeat"])
        if any(key in values for values in self._broadcaster_cache.values()):
            return True
        if payload["height"] not in self._broadcaster_cache:
            self._broadcaster_cache[payload["height"]] = []
        self._broadcaster_cache[payload["height"]].append(key)
        return False
