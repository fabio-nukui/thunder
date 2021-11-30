from cosmos_sdk.core.gamm import MsgSwapExactAmountIn, MsgSwapExactAmountOut
from cosmos_sdk.core.tx import Tx

from ..tx_filter import Filter


class FilterSwap(Filter):
    def __init__(self, pool_id: int):
        self.pool_id = pool_id

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(pool_id={self.pool_id})"

    def match_tx(self, tx: Tx) -> bool:
        for msg in tx.body.messages:
            if not isinstance(msg, (MsgSwapExactAmountIn, MsgSwapExactAmountOut)):
                continue
            for route in msg.routes:
                if route.pool_id == self.pool_id:
                    return True
        return False
