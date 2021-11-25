from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Sequence, TypeVar

from osmosis_proto.osmosis.gamm.v1beta1 import SwapAmountInRoute
from terra_sdk.core import AccAddress
from terra_sdk.core.tx import Tx
from terra_sdk.core.wasm import MsgExecuteContract

from exceptions import MaxSpreadAssertion

from .client import OsmosisClient
from .token import OsmosisNativeToken, OsmosisToken, OsmosisTokenAmount

Operation = tuple[OsmosisTokenAmount, list[MsgExecuteContract]]

_BaseOsmoLiquidityPoolT = TypeVar("_BaseOsmoLiquidityPoolT", bound="BaseOsmosisLiquidityPool")

PRECISION = 18
ROUND_PREC = Decimal(1) / 10 ** PRECISION


class BaseOsmosisLiquidityPool(ABC):
    client: OsmosisClient
    tokens: Sequence[OsmosisToken]
    _stop_updates: bool

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.repr_symbol})"

    @property
    def repr_symbol(self) -> str:
        return "/".join(t.repr_symbol for t in self.tokens)

    @property
    def sorted_tokens(self) -> tuple[OsmosisToken, ...]:
        return tuple(sorted(self.tokens))

    @abstractmethod
    async def op_swap_exact_in(
        self,
        sender: AccAddress,
        amount_in: OsmosisTokenAmount,
        token_out: OsmosisNativeToken,
        safety_margin: bool | int = True,
    ) -> Operation:
        ...

    @abstractmethod
    async def get_amount_out_exact_in(
        self,
        amount_in: OsmosisTokenAmount,
        token_out: OsmosisNativeToken,
        safety_margin: bool | int = False,
    ) -> OsmosisTokenAmount:
        ...

    @abstractmethod
    async def simulate_reserve_change(
        self: _BaseOsmoLiquidityPoolT,
        amounts: list[OsmosisTokenAmount],
    ) -> _BaseOsmoLiquidityPoolT:
        ...

    @abstractmethod
    async def get_reserve_changes_from_msg(self, msg: dict) -> list[OsmosisTokenAmount]:
        ...

    async def get_reserve_changes_from_tx(self, tx: Tx) -> list[OsmosisTokenAmount]:
        acc_changes = [t.to_amount(0) for t in self.tokens]
        errors = []
        for msg in tx.body.messages:
            try:
                tx_changes = await self.get_reserve_changes_from_msg(msg.to_data())
                acc_changes = [c + tx_c for c, tx_c in zip(acc_changes, tx_changes)]
            except MaxSpreadAssertion:
                raise
            except Exception as e:
                if len(tx.body.messages) == 1:
                    raise e
                errors.append(e)
        if not any(acc_changes):
            raise Exception(f"Error when parsing msgs: {errors}")
        return acc_changes


class GAMMLiquidityPool(BaseOsmosisLiquidityPool):
    tokens: list[OsmosisNativeToken]
    pool_id: int
    address: str
    swap_fee: Decimal
    exit_fee: Decimal
    weights: dict[OsmosisNativeToken, Decimal]
    _reserves: dict[OsmosisNativeToken, Decimal]

    @classmethod
    async def new(cls, pool_id: int, client: OsmosisClient) -> GAMMLiquidityPool:
        self = super().__new__(cls)
        self.pool_id = pool_id
        self.client = client
        self._stop_updates = False

        pool = await client.gamm.get_pool(pool_id=pool_id)
        self.address = pool.address
        self.swap_fee = Decimal(pool.pool_params.swap_fee) / 10 ** PRECISION
        self.exit_fee = Decimal(pool.pool_params.exit_fee) / 10 ** PRECISION

        self.tokens = []
        self._reserves = {}
        self.weights = {}
        for asset in pool.pool_assets:
            token = OsmosisNativeToken(asset.token.denom, client)
            self.tokens.append(token)
            self._reserves[token] = token.decimalize(asset.token.amount)
            self.weights[token] = Decimal(asset.weight)

        return self

    async def get_reserves(self):
        if not self._stop_updates:
            self._reserves = await self._get_reserves()
        return self._reserves

    async def _get_reserves(self) -> dict[OsmosisNativeToken, Decimal]:
        assets = await self.client.gamm.get_pool_assets(pool_id=self.pool_id)
        return {t: t.decimalize(a.token.amount) for t, a in zip(self.tokens, assets)}

    async def op_swap_exact_in(
        self,
        sender: AccAddress,
        amount_in: OsmosisTokenAmount,
        token_out: OsmosisNativeToken,
        safety_margin: bool | int = True,
    ) -> Operation:
        raise NotImplementedError

    async def get_amount_out_exact_in(
        self,
        amount_in: OsmosisTokenAmount,
        token_out: OsmosisNativeToken,
        safety_margin: bool | int = False,
    ) -> OsmosisTokenAmount:
        """Get amount out for SwapExactAmountIn
        https://github.com/osmosis-labs/osmosis/blob/main/x/gamm/keeper/math.go#L58
        """
        assert isinstance(amount_in.token, OsmosisNativeToken)

        reserves = await self.get_reserves()
        reserve_in = reserves[amount_in.token]
        reserve_out = reserves[token_out]

        weight_in = self.weights[amount_in.token]
        weight_out = self.weights[token_out]

        adjusted_in = amount_in.amount * (1 - self.swap_fee)
        y = (reserve_in / (reserve_in + adjusted_in)).quantize(ROUND_PREC, "ROUND_DOWN")
        bar = (1 - y ** (weight_in / weight_out)).quantize(ROUND_PREC, "ROUND_DOWN")

        amount_out = token_out.to_amount(reserve_out * bar)
        return amount_out.safe_margin(safety_margin)

    async def estimate_swap_exact_in(
        self,
        amount_in: OsmosisTokenAmount,
        token_out: OsmosisNativeToken,
    ) -> OsmosisTokenAmount:
        route = SwapAmountInRoute(self.pool_id, token_out.denom)
        res = await self.client.grpc_gamm.estimate_swap_exact_amount_in(
            sender=self.client.address,
            pool_id=self.pool_id,
            token_in=amount_in.to_str(),
            routes=[route],
        )
        return token_out.to_amount(int_amount=res.token_out_amount)

    async def simulate_reserve_change(
        self: _BaseOsmoLiquidityPoolT,
        amounts: list[OsmosisTokenAmount],
    ) -> _BaseOsmoLiquidityPoolT:
        raise NotImplementedError

    async def get_reserve_changes_from_msg(self, msg: dict) -> list[OsmosisTokenAmount]:
        raise NotImplementedError
