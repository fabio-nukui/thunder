import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from functools import partial

from terra_sdk.core.auth import StdFee, TxLog
from terra_sdk.core.wasm import MsgExecuteContract
from terra_sdk.exceptions import LCDResponseError

import utils
from chains.terra import LUNA, UST, TerraClient, TerraTokenAmount, terraswap
from exceptions import TxError, UnprofitableArbitrage

from .common.terra_single_tx_arbitrage import TerraArbParams, TerraSingleTxArbitrage

log = logging.getLogger(__name__)

MIN_PROFIT_UST = UST.to_amount(2)
MIN_START_AMOUNT = UST.to_amount(200)
OPTIMIZATION_TOLERANCE = UST.to_amount("0.01")
MAX_SLIPPAGE = Decimal("0.001")


class Direction(str, Enum):
    terraswap_first = "terraswap_first"
    native_first = "native_first"


@dataclass
class ArbParams(TerraArbParams):
    timestamp_found: float
    block_found: int

    prices: dict[str, Decimal]
    terra_virtual_pools: tuple[Decimal, Decimal]
    pool_reserves: tuple[TerraTokenAmount, TerraTokenAmount]
    ust_balance: Decimal
    direction: Direction

    initial_amount: TerraTokenAmount
    msgs: list[MsgExecuteContract]
    est_final_amount: TerraTokenAmount
    est_fee: StdFee
    est_net_profit_usd: Decimal

    def to_data(self) -> dict:
        return {
            "timestamp_found": self.timestamp_found,
            "block_found": self.block_found,
            "prices": {key: float(price) for key, price in self.prices.items()},
            "terra_virtual_pools": [float(vp) for vp in self.terra_virtual_pools],
            "pool_reserves": [reserve.to_data() for reserve in self.pool_reserves],
            "direction": self.direction,
            "initial_amount": self.initial_amount.to_data(),
            "msgs": [msg.to_data() for msg in self.msgs],
            "est_final_amount": self.est_final_amount.to_data(),
            "est_fee": self.est_fee.to_data(),
            "est_net_profit_usd": float(self.est_net_profit_usd),
        }


class LunaUstMarketStrategy(TerraSingleTxArbitrage):
    def __init__(self, client: TerraClient, router: terraswap.Router) -> None:
        self.router = router
        (self.terraswap_pool,) = router.pairs.values()
        self._route_native_first: list[terraswap.RouteStep] = [
            terraswap.RouteStepNative(UST, LUNA),
            terraswap.RouteStepTerraswap(LUNA, UST),
        ]
        self._route_terraswap_first: list[terraswap.RouteStep] = [
            terraswap.RouteStepTerraswap(UST, LUNA),
            terraswap.RouteStepNative(LUNA, UST),
        ]

        super().__init__(client)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(client={self.client}, state={self.state})"

    def _get_arbitrage_params(self, block: int, mempool: dict = None) -> ArbParams:
        if mempool:
            raise NotImplementedError
        prices = self._get_prices()
        terraswap_premium = prices["terraswap"] / prices["market"] - 1
        if terraswap_premium > 0:
            direction = Direction.native_first
            route = self._route_native_first
        else:
            direction = Direction.terraswap_first
            route = self._route_terraswap_first
        ust_balance = UST.get_balance(self.client).amount

        initial_amount = self._get_optimal_argitrage_amount(route, terraswap_premium, ust_balance)
        final_amount, msgs = self._op_arbitrage(initial_amount, route)
        try:
            fee = self.client.tx.estimate_fee(msgs)
        except LCDResponseError as e:
            log.debug(
                "Error when estimating fee",
                extra={
                    "data": {
                        "terraswap_premium": f"{terraswap_premium:.3%}",
                        "direction": direction,
                        "msgs": [msg.to_data() for msg in msgs],
                    },
                },
                exc_info=True,
            )
            raise TxError(e)
        gas_cost = TerraTokenAmount.from_coin(*fee.amount)
        net_profit_ust = final_amount - initial_amount - gas_cost
        if net_profit_ust < MIN_PROFIT_UST:
            raise UnprofitableArbitrage(
                f"Low profitability: USD {net_profit_ust:.2f}, {terraswap_premium=:0.3%}"
            )

        return ArbParams(
            timestamp_found=time.time(),
            block_found=block,
            prices=prices,
            terra_virtual_pools=self.client.market.virtual_pools,
            ust_balance=ust_balance,
            pool_reserves=self.terraswap_pool.reserves,
            direction=direction,
            initial_amount=initial_amount,
            msgs=msgs,
            est_final_amount=final_amount,
            est_fee=fee,
            est_net_profit_usd=net_profit_ust.amount,
        )

    def _get_prices(self) -> dict[str, Decimal]:
        terraswap_price = (
            self.terraswap_pool.reserves[0].amount / self.terraswap_pool.reserves[1].amount
        )
        market_price = self.client.oracle.exchange_rates[UST]
        return {
            "terraswap": terraswap_price,
            "market": market_price,
        }

    def _get_optimal_argitrage_amount(
        self,
        route: list[terraswap.RouteStep],
        terraswap_premium: Decimal,
        ust_balance: Decimal,
    ) -> TerraTokenAmount:
        profit = self._get_gross_profit(MIN_START_AMOUNT, route)
        if profit < 0:
            raise UnprofitableArbitrage(f"No profitability, {terraswap_premium=:0.3%}")
        func = partial(self._get_gross_profit_dec, route=route)
        ust_amount, _ = utils.optimization.optimize(
            func,
            x0=MIN_START_AMOUNT.amount,
            dx=MIN_START_AMOUNT.dx,
            tol=OPTIMIZATION_TOLERANCE.amount,
        )
        if ust_amount > ust_balance:
            log.warning(
                "Not enough balance for full arbitrage: "
                f"wanted UST {ust_amount:,.2f}, have UST {ust_balance:,.2f}"
            )
            return UST.to_amount(ust_balance)
        return UST.to_amount(ust_amount)

    def _get_gross_profit(
        self,
        initial_lp_amount: TerraTokenAmount,
        route: list[terraswap.RouteStep],
    ) -> TerraTokenAmount:
        amount_out, _ = self._op_arbitrage(initial_lp_amount, route)
        return amount_out - initial_lp_amount

    def _get_gross_profit_dec(
        self,
        amount: Decimal,
        route: list[terraswap.RouteStep],
    ) -> Decimal:
        token_amount = UST.to_amount(amount)
        return self._get_gross_profit(token_amount, route).amount

    def _op_arbitrage(
        self,
        initial_ust_amount: TerraTokenAmount,
        route: list[terraswap.RouteStep],
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        return self.router.op_route_swap(
            self.client.address, initial_ust_amount, route, MAX_SLIPPAGE
        )

    def _extract_returns_from_logs(self, logs: list[TxLog]) -> tuple[TerraTokenAmount, Decimal]:
        tx_events = TerraClient.extract_log_events(logs)
        logs_from_contract = TerraClient.parse_from_contract_events(tx_events)
        log.debug(logs_from_contract)
        return UST.to_amount(), Decimal(0)  # TODO: implement


def run():
    client = TerraClient()
    pool_addresses = terraswap.get_addresses(client.chain_id)
    pairs = [terraswap.LiquidityPair(pool_addresses["pools"]["ust_luna"], client)]
    router = terraswap.Router(pairs, client)

    strategy = LunaUstMarketStrategy(client, router)
    for block in client.wait_next_block():
        strategy.run(block)
        utils.cache.clear_caches(utils.cache.CacheGroup.TERRA)
