from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal, NamedTuple, Sequence, TypedDict

from terra_sdk.core.auth import StdFee
from terra_sdk.core.broadcast import SyncTxBroadcastResult
from terra_sdk.core.msg import Msg

import utils
from exceptions import TxAlreadyBroadcasted

from .base_api import Api

if TYPE_CHECKING:
    from .async_client import TerraClient

log = logging.getLogger(__name__)

BROADCASTER_CACHE_BLOCKS = 3


class BroadcasterPayload(TypedDict):
    height: int
    msgs: list[dict]
    n_repeat: int
    expect_logs: bool
    fee: dict | None
    fee_denom: str | None


class BroadcasterResponse(TypedDict):
    result: Literal["broadcasted"] | Literal["repeated_tx"]
    data: list[tuple[float, dict]]


class BroadcastCacheKey(NamedTuple):
    msgs: list[dict]
    n_repeat: int


class BroadcasterApi(Api):
    def __init__(self, client: "TerraClient"):
        super().__init__(client)
        self._broadcaster_cache: dict[int, list[BroadcastCacheKey]] = {}

    async def start(self):
        self._http_client = utils.ahttp.AsyncClient(base_url=self.client.broadcaster_uri)

    async def close(self):
        await self._http_client.aclose()

    async def post(
        self,
        msgs: Sequence[Msg],
        n_repeat: int,
        expect_logs: bool,
        fee: StdFee = None,
        fee_denom: str = None,
    ) -> list[tuple[float, SyncTxBroadcastResult]]:
        payload: BroadcasterPayload = {
            "height": self.client.height,
            "msgs": [m.to_data() for m in msgs],
            "n_repeat": n_repeat,
            "expect_logs": expect_logs,
            "fee": fee.to_data() if fee is not None else None,
            "fee_denom": fee_denom,
        }
        res = await self._http_client.post("txs", json=payload)
        data: BroadcasterResponse = res.json()
        if data["result"] == "repeated_tx":
            raise TxAlreadyBroadcasted
        return [(timestamp, SyncTxBroadcastResult(**result)) for timestamp, result in data["data"]]

    async def broadcast(self, payload: BroadcasterPayload) -> BroadcasterResponse:
        assert not self.client.use_broadcaster
        if self._is_repeated_tx(payload):
            return {"result": "repeated_tx", "data": []}
        msgs = [Msg.from_data(d) for d in payload["msgs"]]
        n_repeat = payload["n_repeat"]
        expect_logs = payload["expect_logs"]
        fee = StdFee.from_data(payload["fee"]) if payload["fee"] is not None else None
        fee_denom = payload["fee_denom"]

        try:
            res = await self.client.tx.execute_multi_msgs(
                msgs, n_repeat, expect_logs, fee=fee, fee_denom=fee_denom
            )
        except TxAlreadyBroadcasted:
            return {"result": "repeated_tx", "data": []}
        return {
            "result": "broadcasted",
            "data": [(timestamp, result.to_data()) for timestamp, result in res],
        }

    def _is_repeated_tx(self, payload: BroadcasterPayload) -> bool:
        self._broadcaster_cache = {  # Drop old values
            height: val
            for height, val in self._broadcaster_cache.items()
            if payload["height"] - height <= BROADCASTER_CACHE_BLOCKS
        }
        key = BroadcastCacheKey(payload["msgs"], payload["n_repeat"])
        if any(key in values for values in self._broadcaster_cache.values()):
            return True
        if payload["height"] not in self._broadcaster_cache:
            self._broadcaster_cache[payload["height"]] = []
        self._broadcaster_cache[payload["height"]].append(key)
        return False
