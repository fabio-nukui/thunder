import logging
from contextlib import asynccontextmanager
from typing import Iterable

import utils
from chains.terra import terraswap
from chains.terra.token import TerraTokenAmount
from exceptions import MaxSpreadAssertion

log = logging.getLogger(__name__)

AmountTuple = tuple[TerraTokenAmount, TerraTokenAmount]


class TerraswapLPReserveSimulationMixin:
    log: utils.logger.ReformatedLogger

    def __init__(self, *args, pairs: Iterable[terraswap.HybridLiquidityPair], **kwargs):
        self.pairs = pairs
        self._mempool_reserve_changes = self._get_initial_mempool_params()

        super().__init__(*args, **kwargs)  # type: ignore

    def _get_initial_mempool_params(self) -> dict[terraswap.HybridLiquidityPair, AmountTuple]:
        return {
            pair: (pair.tokens[0].to_amount(0), pair.tokens[1].to_amount(0)) for pair in self.pairs
        }

    def _reset_mempool_params(self):
        self._mempool_reserve_changes = self._get_initial_mempool_params()

    @property
    def _simulating_reserve_changes(self) -> bool:
        return any(pair._stop_updates for pair in self.pairs)

    @asynccontextmanager
    async def _simulate_reserve_changes(
        self,
        filtered_mempool: dict[terraswap.HybridLiquidityPair, list[list[dict]]] = None,
    ):
        if filtered_mempool is None:
            yield
            return
        if not any(list_msgs for list_msgs in filtered_mempool.values()):
            yield
            return
        for pair, list_msgs in filtered_mempool.items():
            for (msg,) in list_msgs:  # Only txs with one message were filtered
                try:
                    changes = await pair.get_reserve_changes_from_msg(msg["value"])
                except MaxSpreadAssertion:
                    continue
                self._mempool_reserve_changes[pair] = (
                    self._mempool_reserve_changes[pair][0] + changes[0],
                    self._mempool_reserve_changes[pair][1] + changes[1],
                )
        simulations: dict[terraswap.HybridLiquidityPair, terraswap.HybridLiquidityPair] = {}
        for pair in self.pairs:
            pair_changes = self._mempool_reserve_changes[pair]
            if any(amount for amount in pair_changes):
                self.log.debug(f"Simulation of reserve changes: {pair}: {pair_changes}")
                simulations[pair] = await pair.simulate_reserve_change(pair_changes)
            else:
                simulations[pair] = pair
        pairs = self.pairs
        try:
            self.pairs = simulations.values()
            yield
        finally:
            self.pairs = pairs
