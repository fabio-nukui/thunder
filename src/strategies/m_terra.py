import asyncio
from typing import Sequence

from arbitrage.cosmos import CosmosRepeatedTxArbitrage, run_strategy
from chains.cosmos.terra import TerraClient
from utils.cache import CacheGroup

from . import s_lp_tower, s_terra_cycles


async def run(max_n_blocks: int = None):
    async with TerraClient() as client:
        terra_cycle_arbs, s_lp_tower_arbs = await asyncio.gather(
            s_terra_cycles.get_arbitrages(client),
            s_lp_tower.get_arbitrages(client),
        )
        terra_cycles = s_terra_cycles.get_filters(terra_cycle_arbs)
        s_lp_tower_filters = s_lp_tower.get_filters(s_lp_tower_arbs)

        arb_routes: Sequence[CosmosRepeatedTxArbitrage[TerraClient]] = [
            *terra_cycle_arbs,
            *s_lp_tower_arbs,
        ]
        mempool_filters = terra_cycles | s_lp_tower_filters

        await run_strategy(client, arb_routes, mempool_filters, CacheGroup.TERRA, max_n_blocks)
