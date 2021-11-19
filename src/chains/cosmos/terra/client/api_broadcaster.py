from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal, Sequence, TypedDict

from terra_sdk.core.broadcast import SyncTxBroadcastResult
from terra_sdk.core.fee import Fee
from terra_sdk.core.msg import Msg

from exceptions import BlockchainNewState, TxAlreadyBroadcasted

from .base_api import Api

if TYPE_CHECKING:
    from .async_client import TerraClient

log = logging.getLogger(__name__)


class BroadcasterPayload(TypedDict):
    height: int
    msgs: list[dict]
    n_repeat: int
    fee: dict | None
    fee_denom: str | None


class BroadcasterResponse(TypedDict):
    result: Literal["broadcasted"] | Literal["repeated_tx"] | Literal["new_block"]
    data: list[tuple[float, dict]]


def _extract_signature(msgs: list[dict]) -> set[str]:
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


class BroadcasterApi(Api):
    def __init__(self, client: "TerraClient"):
        super().__init__(client)
        self._height: int = 0
        self._broadcasted_signatures: set[str] = set()

    async def post(
        self,
        msgs: Sequence[Msg],
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
            self._broadcasted_signatures = set()
        elif payload["height"] < self._height:
            return {"result": "new_block", "data": []}

        msg_signature = _extract_signature(payload["msgs"])
        if self._broadcasted_signatures & msg_signature:
            return {"result": "repeated_tx", "data": []}
        self._broadcasted_signatures |= msg_signature

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
