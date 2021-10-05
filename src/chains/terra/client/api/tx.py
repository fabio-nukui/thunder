import logging
from typing import Callable, TypeVar

from terra_sdk.core import Coins
from terra_sdk.core.auth import StdFee, StdTx
from terra_sdk.core.broadcast import (AsyncTxBroadcastResult, BlockTxBroadcastResult,
                                      SyncTxBroadcastResult)
from terra_sdk.core.msg import Msg

import utils
from utils.cache import CacheGroup, ttl_cache

from ...core import BaseTxApi

log = logging.getLogger(__name__)

TERRA_GAS_PRICE_CACHE_TTL = 3600

_BroadcastResutT = TypeVar(
    '_BroadcastResutT',
    BlockTxBroadcastResult,
    SyncTxBroadcastResult,
    AsyncTxBroadcastResult,
)


class TxApi(BaseTxApi):
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=TERRA_GAS_PRICE_CACHE_TTL)
    def get_gas_prices(self) -> Coins:
        res = utils.http.get(f'{self.client.fcd_uri}/v1/txs/gas_prices')
        return Coins(res.json())

    def estimate_fee(
        self,
        msgs: list[Msg],
        gas_adjustment: float = None,
    ) -> StdFee:
        return self.client.lcd.tx.estimate_fee(
            self.client.address,
            msgs,
            gas_adjustment=gas_adjustment,
            fee_denoms=[self.client.fee_denom],
        )

    def execute_msgs_block(self, msgs: list[Msg], **kwargs) -> BlockTxBroadcastResult:
        return self._execute_msgs(msgs, broadcast_func=self.client.lcd.tx.broadcast, **kwargs)

    def execute_msgs_sync(self, msgs: list[Msg], **kwargs) -> SyncTxBroadcastResult:
        return self._execute_msgs(msgs, broadcast_func=self.client.lcd.tx.broadcast_sync, **kwargs)

    def execute_msgs_async(self, msgs: list[Msg], **kwargs) -> AsyncTxBroadcastResult:
        # return self._execute_msgs(msgs, broadcast_func=self.lcd.tx.broadcast_async, **kwargs)
        return self._execute_msgs(msgs, broadcast_func=self._broadcast_async, **kwargs)

    def _execute_msgs(
        self,
        msgs: list,
        broadcast_func: Callable[[StdTx], _BroadcastResutT],
        **kwargs,
    ) -> _BroadcastResutT:
        log.debug(f'Sending tx: {msgs}')
        signed_tx = self.client.wallet.create_and_sign_tx(
            msgs,
            fee_denoms=[self.client.fee_denom],
            **kwargs,
        )

        res = broadcast_func(signed_tx)
        log.debug(f'Tx executed: {res.txhash}')
        return res

    def _broadcast_async(self, tx: StdTx) -> AsyncTxBroadcastResult:
        payload = {'tx': tx.to_data()['value'], 'mode': 'async'}
        res = utils.http.post(f'{self.client.lcd_uri}/txs', json=payload)

        return AsyncTxBroadcastResult(txhash=res.json()['txhash'], height=self.client.block)
