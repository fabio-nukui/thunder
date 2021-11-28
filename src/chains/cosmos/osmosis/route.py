from .data import SwapAmountInRoute
from .liquidity_pair import GAMMLiquidityPool
from .token import OsmosisNativeToken


class RoutePools:
    def __init__(
        self,
        tokens: list[OsmosisNativeToken],
        pools: list[GAMMLiquidityPool],
    ):
        assert len(tokens) == len(pools) + 1
        assert tokens[0] in pools[0].tokens

        self.start_token = tokens[0]
        self.tokens = tokens
        self.pools = pools

        self.routes: list[SwapAmountInRoute] = []
        for pool, token in zip(pools, tokens[1:]):
            assert token in pool.tokens
            self.routes.append(SwapAmountInRoute(pool.pool_id, token.denom))
