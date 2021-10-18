import asyncio
from typing import Sequence

from chains.terra import TerraClient

from . import lp_tower, luna_ust_market, ust_cycles
from .common.single_tx_arbitrage import SingleTxArbitrage
from .common.terra_single_tx_arbitrage import run_strategy


async def run(max_n_blocks: int = None):
    async with await TerraClient.new() as client:
        lp_tower_arbs, luna_market_arbs, ust_cycles_arbs = await asyncio.gather(
            lp_tower.get_arbitrages(client),
            luna_ust_market.get_arbitrages(client),
            ust_cycles.get_arbitrages(client),
        )
        lp_tower_filters = lp_tower.get_filters(lp_tower_arbs)
        luna_market_filters = luna_ust_market.get_filters(luna_market_arbs)
        ust_cycles_filters = ust_cycles.get_filters(ust_cycles_arbs)

        arb_routes: Sequence[SingleTxArbitrage] = [
            *lp_tower_arbs,
            *luna_market_arbs,
            *ust_cycles_arbs,
        ]
        mempool_filters = lp_tower_filters | luna_market_filters | ust_cycles_filters

        await run_strategy(client, arb_routes, mempool_filters, max_n_blocks)
