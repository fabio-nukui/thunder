import logging
from contextlib import AsyncExitStack, asynccontextmanager

from chains.terra import terraswap
from chains.terra.token import TerraTokenAmount
from exceptions import MaxSpreadAssertion

log = logging.getLogger(__name__)

AmountTuple = tuple[TerraTokenAmount, TerraTokenAmount]


class TerraswapLPReserveSimulationMixin:
    pairs: list[terraswap.LiquidityPair]

    def __init__(self, *args, **kwargs) -> None:
        self._simulating_reserve_changes = False
        self._mempool_reserve_changes = self._get_initial_mempool_params()

        super().__init__(*args, **kwargs)

    def _get_initial_mempool_params(self) -> dict[terraswap.LiquidityPair, AmountTuple]:
        return {
            pair: (pair.tokens[0].to_amount(0), pair.tokens[1].to_amount(0)) for pair in self.pairs
        }

    @asynccontextmanager
    async def _simulate_reserve_changes(
        self,
        filtered_mempool: dict[terraswap.LiquidityPair, list[list[dict]]] = None,
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
        async with AsyncExitStack() as stack:
            for pair in self.pairs:
                pair_changes = self._mempool_reserve_changes[pair]
                if any(amount for amount in pair_changes):
                    log.debug(f"{self}: Simulation of reserve changes: {pair}: {pair_changes}")
                    await stack.enter_async_context(pair.simulate_reserve_change(pair_changes))

            simulating_reserve_changes = self._simulating_reserve_changes
            self._simulating_reserve_changes = True
            try:
                yield
            finally:
                self._simulating_reserve_changes = simulating_reserve_changes
