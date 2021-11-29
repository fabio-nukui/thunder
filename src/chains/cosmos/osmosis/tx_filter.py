from cosmos_sdk.core.tx import Tx

from ..tx_filter import Filter
from .liquidity_pair import GAMMLiquidityPool


class FilterSwap(Filter):
    def __init__(self, pool: GAMMLiquidityPool):
        self.pool = pool

    def match_tx(self, tx: Tx) -> bool:
        raise NotImplementedError
