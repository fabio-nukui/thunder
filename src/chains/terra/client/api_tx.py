import logging
import re

from terra_sdk.core import Coins
from terra_sdk.core.auth import StdFee
from terra_sdk.core.broadcast import AsyncTxBroadcastResult
from terra_sdk.core.msg import Msg
from terra_sdk.exceptions import LCDResponseError

from utils.cache import CacheGroup, ttl_cache

from ..interfaces import ITxApi

log = logging.getLogger(__name__)

TERRA_GAS_PRICE_CACHE_TTL = 3600

_pat_sequence_error = re.compile(r"account sequence mismatch, expected (\d+)")


class TxApi(ITxApi):
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=TERRA_GAS_PRICE_CACHE_TTL)
    async def get_gas_prices(self) -> Coins:
        res = await self.client.fcd_client.get("v1/txs/gas_prices")
        return Coins(res.json())

    async def estimate_fee(
        self,
        msgs: list[Msg],
        gas_adjustment: float = None,
    ) -> StdFee:
        return await self.client.lcd.tx.estimate_fee(
            self.client.address,
            msgs,
            gas_adjustment=gas_adjustment,
            fee_denoms=[self.client.fee_denom],
        )

    async def execute_msgs(self, msgs: list[Msg], **kwargs) -> AsyncTxBroadcastResult:
        log.debug(f"Sending tx: {msgs}")

        # Fixes bug in terraswap_sdk==1.0.0b2
        if "fee" not in kwargs:
            kwargs["fee"] = self.estimate_fee(msgs)

        while True:
            signed_tx = await self.client.wallet.create_and_sign_tx(
                msgs,
                fee_denoms=[self.client.fee_denom],
                **kwargs,
            )
            payload = {"tx": signed_tx.to_data()["value"], "mode": "async"}
            try:
                res = (await self.client.lcd_http_client.post("txs", json=payload)).json()
                break
            except LCDResponseError as e:
                if match := _pat_sequence_error.search(e.message):
                    kwargs["sequence"] = int(match.group(1))
        log.debug(f"Tx executed: {res['txhash']}")
        return AsyncTxBroadcastResult(txhash=res["txhash"], height=self.client.height)
