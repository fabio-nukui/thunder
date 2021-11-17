from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Callable, Iterable, Sequence

import utils
from chains.cosmos.terra import BaseTerraLiquidityPair, TerraTokenAmount
from chains.cosmos.terra.swap_utils import SingleRoute
from exceptions import MaxSpreadAssertion

log = logging.getLogger(__name__)

AmountTuple = tuple[TerraTokenAmount, TerraTokenAmount]
PairsCls = Callable[[Iterable[BaseTerraLiquidityPair]], Sequence[BaseTerraLiquidityPair]]


class TerraswapLPReserveSimulationMixin:
    log: utils.logger.ReformatedLogger

    def __init__(
        self,
        *args,
        pairs: Sequence[BaseTerraLiquidityPair],
        pairs_cls: PairsCls = list,
        routes: list[SingleRoute] = None,
        **kwargs,
    ):
        self.pairs = pairs
        self.pairs_cls = pairs_cls
        self.routes = routes or []
        self._mempool_reserve_changes = self._get_initial_mempool_params()

        super().__init__(*args, **kwargs)  # type: ignore

    def _get_initial_mempool_params(self) -> dict[BaseTerraLiquidityPair, AmountTuple]:
        return {
            pair: (pair.tokens[0].to_amount(0), pair.tokens[1].to_amount(0))
            for pair in self.pairs
        }

    def _reset_mempool_params(self):
        self._mempool_reserve_changes = self._get_initial_mempool_params()

    @property
    def _simulating_reserve_changes(self) -> bool:
        return any(pair._stop_updates for pair in self.pairs)

    @asynccontextmanager
    async def _simulate_reserve_changes(
        self,
        filtered_mempool: dict[BaseTerraLiquidityPair, list[list[dict]]] = None,
    ):
        if not filtered_mempool:
            yield self.pairs
            return
        for pair, list_msgs in filtered_mempool.items():
            for msgs in list_msgs:
                try:
                    changes = await pair.get_reserve_changes_from_msgs(msgs)
                except MaxSpreadAssertion as e:
                    log.debug(f"{pair}: {e!r}")
                    continue
                except Exception:
                    log.exception(f"Error when decoding {msgs}")
                    continue
                self._mempool_reserve_changes[pair] = (
                    self._mempool_reserve_changes[pair][0] + changes[0],
                    self._mempool_reserve_changes[pair][1] + changes[1],
                )
        simulations: dict[BaseTerraLiquidityPair, BaseTerraLiquidityPair] = {}
        for pair in self.pairs:
            pair_changes = self._mempool_reserve_changes[pair]
            if any(amount for amount in pair_changes):
                self.log.debug(f"Simulation of reserve changes: {pair}: {pair_changes}")
                simulations[pair] = await pair.simulate_reserve_change(pair_changes)
            else:
                simulations[pair] = pair
        pairs = self.pairs
        route_pairs = {route: route.pairs for route in self.routes}
        try:
            for route in self.routes:
                route.pairs = [simulations[pair] for pair in route.pairs]
            self.pairs = self.pairs_cls(simulations.values())
            yield
        finally:
            self.pairs = pairs
            for route in self.routes:
                route.pairs = route_pairs[route]
