from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Callable, Iterable, Sequence, Union

from terra_sdk.core.tx import Tx

from chains.cosmos.osmosis import GAMMLiquidityPool
from chains.cosmos.osmosis.route import RoutePools as OsmosisRoute
from chains.cosmos.terra import BaseTerraLiquidityPair
from chains.cosmos.terra.route import RoutePools as TerraRoute
from chains.cosmos.token import CosmosTokenAmount
from exceptions import MaxSpreadAssertion

log = logging.getLogger(__name__)
LiquidityPool = Union[BaseTerraLiquidityPair, GAMMLiquidityPool]
PoolsCls = Callable[[Iterable[LiquidityPool]], Sequence[LiquidityPool]]
RoutePools = Union[TerraRoute, OsmosisRoute]


class LPReserveSimulationMixin:
    log: logging.Logger

    def __init__(
        self,
        *args,
        pools: Sequence[LiquidityPool],
        pool_cls: PoolsCls = list,
        routes: Sequence[RoutePools] = None,
        **kwargs,
    ):
        self.pools = pools
        self.pool_cls = pool_cls
        self.routes = routes or []
        self._mempool_reserve_changes = self._get_initial_mempool_params()

        super().__init__(*args, **kwargs)  # type: ignore

    def _get_initial_mempool_params(self) -> dict[LiquidityPool, list[CosmosTokenAmount]]:
        return {pool: [t.to_amount(0) for t in pool.tokens] for pool in self.pools}

    def _reset_mempool_params(self):
        self._mempool_reserve_changes = self._get_initial_mempool_params()

    @property
    def _simulating_reserve_changes(self) -> bool:
        return any(p.stop_updates for p in self.pools)

    @asynccontextmanager
    async def _simulate_reserve_changes(
        self,
        filtered_mempool: dict[LiquidityPool, list[Tx]] = None,
    ):
        if not filtered_mempool:
            yield self.pools
            return
        for pool, list_txs in filtered_mempool.items():
            for tx in list_txs:
                try:
                    changes = await pool.get_reserve_changes_from_tx(tx)
                except MaxSpreadAssertion as e:
                    log.debug(f"{pool}: {e!r}")
                    continue
                except Exception:
                    log.exception(f"Error when decoding {tx}")
                    continue
                self._mempool_reserve_changes[pool] = [
                    p + c for p, c in zip(self._mempool_reserve_changes[pool], changes)
                ]
        simulations: dict[LiquidityPool, LiquidityPool] = {}
        for pool in self.pools:
            pool_changes = self._mempool_reserve_changes[pool]
            if any(amount for amount in pool_changes):
                self.log.debug(f"Simulation of reserve changes: {pool}: {pool_changes}")
                simulations[pool] = await pool.simulate_reserve_change(pool_changes)  # type: ignore # noqa: E501
            else:
                simulations[pool] = pool
        pools = self.pools
        route_pairs = {route: route.pools for route in self.routes}
        try:
            for route in self.routes:
                route.pools = [simulations[pool] for pool in route.pools]  # type: ignore
            self.pools = self.pool_cls(simulations.values())
            yield
        finally:
            self.pools = pools
            for route in self.routes:
                route.pools = route_pairs[route]  # type: ignore
