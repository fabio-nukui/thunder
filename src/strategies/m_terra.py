import asyncio
from typing import Sequence

from arbitrage.terra import TerraRepeatedTxArbitrage, run_strategy
from chains.terra import TerraClient, terraswap

from . import s_lp_tower, s_terra_cycles


async def run(max_n_blocks: int = None):
    async with TerraClient() as client:
        terra_cycle_arbs, s_lp_tower_arbs = await asyncio.gather(
            s_terra_cycles.get_arbitrages(client),
            s_lp_tower.get_arbitrages(client),
        )
        terraswap_factory = await terraswap.TerraswapFactory.new(client)
        terra_cycles = s_terra_cycles.get_filters(terra_cycle_arbs, terraswap_factory)
        s_lp_tower_filters = s_lp_tower.get_filters(s_lp_tower_arbs)

        arb_routes: Sequence[TerraRepeatedTxArbitrage] = [
            *terra_cycle_arbs,
            *s_lp_tower_arbs,
        ]
        mempool_filters = terra_cycles | s_lp_tower_filters

        await run_strategy(client, arb_routes, mempool_filters, max_n_blocks)
