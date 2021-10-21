import asyncio
from typing import Sequence

from arbitrage.terra import TerraRepeatedTxArbitrage, run_strategy
from chains.terra import TerraClient

from . import s_lp_tower, s_luna_ust_market, s_ust_cycles


async def run(max_n_blocks: int = None):
    async with await TerraClient.new() as client:
        luna_market_arbs, s_ust_cycles_arbs, s_lp_tower_arbs = await asyncio.gather(
            s_luna_ust_market.get_arbitrages(client),
            s_ust_cycles.get_arbitrages(client),
            s_lp_tower.get_arbitrages(client),
        )
        luna_market_filters = s_luna_ust_market.get_filters(luna_market_arbs)
        s_ust_cycles_filters = s_ust_cycles.get_filters(s_ust_cycles_arbs)
        s_lp_tower_filters = s_lp_tower.get_filters(s_lp_tower_arbs)

        arb_routes: Sequence[TerraRepeatedTxArbitrage] = [
            *luna_market_arbs,
            *s_ust_cycles_arbs,
            *s_lp_tower_arbs,
        ]
        mempool_filters = luna_market_filters | s_ust_cycles_filters | s_lp_tower_filters

        await run_strategy(client, arb_routes, mempool_filters, max_n_blocks)
