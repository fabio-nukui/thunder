from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from copy import copy
from decimal import Decimal
from typing import TYPE_CHECKING, Sequence, TypeVar

from cosmos_proto.osmosis.gamm.v1beta1 import Pool, SwapAmountInRoute
from cosmos_sdk.core.gamm import MsgSwapExactAmountIn, MsgSwapExactAmountOut
from cosmos_sdk.core.msg import Msg
from cosmos_sdk.core.tx import Tx
from cosmos_sdk.core.wasm import MsgExecuteContract

from exceptions import InsufficientLiquidity, MaxSpreadAssertion
from utils.cache import CacheGroup, ttl_cache

from .token import OsmosisNativeToken, OsmosisToken, OsmosisTokenAmount

if TYPE_CHECKING:
    from .client import OsmosisClient

log = logging.getLogger(__name__)

Operation = tuple[OsmosisTokenAmount, list[MsgExecuteContract]]

_BaseOsmoLiquidityPoolT = TypeVar("_BaseOsmoLiquidityPoolT", bound="BaseOsmosisLiquidityPool")

PRECISION = 18
_MIN_RESERVE = Decimal("0.01")
_ROUND_RATIO_MUL = Decimal("2")
_ROUND_RATIO_POW = Decimal("1.7")
_MAX_ADJUSTMENT_PCT = Decimal("0.00001")
_RESERVES_CACHE_SIZE = 1000


class BaseOsmosisLiquidityPool(ABC):
    client: OsmosisClient
    tokens: Sequence[OsmosisToken]
    stop_updates: bool

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.repr_symbol})"

    @property
    def repr_symbol(self) -> str:
        return "/".join(t.repr_symbol for t in self.tokens)

    @property
    def sorted_tokens(self) -> tuple[OsmosisToken, ...]:
        return tuple(sorted(self.tokens))

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
    async def get_reserve_changes_from_msg(self, msg: Msg) -> list[OsmosisTokenAmount]:
        ...

    async def get_reserve_changes_from_tx(self, tx: Tx) -> list[OsmosisTokenAmount]:
        acc_changes = [t.to_amount(0) for t in self.tokens]
        errors = []
        for msg in tx.body.messages:
            try:
                tx_changes = await self.get_reserve_changes_from_msg(msg)
                acc_changes = [c + tx_c for c, tx_c in zip(acc_changes, tx_changes)]
            except MaxSpreadAssertion:
                raise
            except Exception as e:
                if len(tx.body.messages) == 1:
                    raise e
                errors.append(e)
        if not any(acc_changes):
            if any(isinstance(msg, MsgSwapExactAmountIn) for msg in tx.body.messages):
                # TODO: remove "if" when MsgSwapExactAmountOut is implemented
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

    __instances: dict[int, GAMMLiquidityPool | Exception] = {}
    __instances_creation: dict[int, asyncio.Event] = {}

    @classmethod
    async def new(cls, pool_id: int, client: OsmosisClient) -> GAMMLiquidityPool:
        pool = await client.gamm.get_pool(pool_id=pool_id)
        self = await cls.from_proto(pool, client)
        if any(r < _MIN_RESERVE for r in self._reserves.values()):
            raise InsufficientLiquidity
        return self

    @classmethod
    async def from_proto(cls, pool: Pool, client: OsmosisClient) -> GAMMLiquidityPool:
        if pool.id in cls.__instances:
            return cls._get_instance(pool.id, client)
        if pool.id in cls.__instances_creation:
            await cls.__instances_creation[pool.id].wait()
            return cls._get_instance(pool.id, client)
        cls.__instances_creation[pool.id] = asyncio.Event()

        self = super().__new__(cls)
        try:
            self.pool_id = pool.id
            self.client = client
            self.stop_updates = False

            self.address = pool.address
            self.swap_fee = Decimal(pool.pool_params.swap_fee) / 10 ** PRECISION
            self.exit_fee = Decimal(pool.pool_params.exit_fee) / 10 ** PRECISION

            self.tokens = []
            self._reserves = {}
            self.weights = {}
            for asset in pool.pool_assets:
                token = OsmosisNativeToken(asset.token.denom, client.chain_id)
                self.tokens.append(token)
                self._reserves[token] = token.decimalize(asset.token.amount)
                self.weights[token] = Decimal(asset.weight)
        except Exception as e:
            cls.__instances[pool.id] = e
        else:
            cls.__instances[pool.id] = self
        finally:
            cls.__instances_creation[pool.id].set()
            del cls.__instances_creation[pool.id]

        return self

    @classmethod
    def _get_instance(cls, pool_id: int, client: OsmosisClient) -> GAMMLiquidityPool:
        instance = cls.__instances[pool_id]
        if isinstance(instance, Exception):
            raise instance
        instance.client = client
        return instance

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.pool_id}, {self.repr_symbol})"

    def __hash__(self) -> int:
        return hash((self.__class__, self.client.chain_id, self.pool_id))

    async def get_reserves(self):
        if not self.stop_updates:
            self._reserves = await self._get_reserves()
        return self._reserves

    @ttl_cache(CacheGroup.OSMOSIS, maxsize=_RESERVES_CACHE_SIZE)
    async def _get_reserves(self) -> dict[OsmosisNativeToken, Decimal]:
        assets = await self.client.gamm.get_pool_assets(pool_id=self.pool_id)
        return {t: t.decimalize(a.token.amount) for t, a in zip(self.tokens, assets)}

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
        y = reserve_in / (reserve_in + adjusted_in)
        bar = 1 - y ** (weight_in / weight_out)

        amount_out = token_out.to_amount(reserve_out * bar)

        if weight_in != weight_out:
            # Apply aditional safety margin due to cosmos-sdk Pow() implementation
            ratio = (reserve_out / weight_out) / (reserve_in / weight_in)
            adjustment = token_out.decimalize(_ROUND_RATIO_MUL * ratio ** _ROUND_RATIO_POW)
            amount_out -= min(adjustment, amount_out.amount * _MAX_ADJUSTMENT_PCT)

        return amount_out.safe_margin(safety_margin)

    async def get_amount_in_exact_out(
        self,
        token_in: OsmosisNativeToken,
        amount_out: OsmosisTokenAmount,
        safety_margin: bool | int = False,
    ) -> OsmosisTokenAmount:
        raise NotImplementedError

    async def estimate_swap_exact_in(
        self,
        amount_in: OsmosisTokenAmount,
        token_out: OsmosisNativeToken,
    ) -> OsmosisTokenAmount:
        route = SwapAmountInRoute(self.pool_id, token_out.denom)
        res = await self.client.gamm.grpc_query.estimate_swap_exact_amount_in(
            sender=self.client.address,
            pool_id=self.pool_id,
            token_in=amount_in.to_str(),
            routes=[route],
        )
        return token_out.to_amount(int_amount=res.token_out_amount)

    async def simulate_reserve_change(
        self: GAMMLiquidityPool,
        amounts: list[OsmosisTokenAmount],
    ) -> GAMMLiquidityPool:
        reserves = await self.get_reserves()
        simulation = copy(self)
        simulation.stop_updates = True
        for amount in amounts:
            assert isinstance(amount.token, OsmosisNativeToken)
            reserves[amount.token] += amount.amount
        simulation._reserves = reserves
        return simulation

    async def get_reserve_changes_from_msg(self, msg: Msg) -> list[OsmosisTokenAmount]:
        changes = {t: t.to_amount(0) for t in self.tokens}
        if isinstance(msg, MsgSwapExactAmountIn):
            if not any(self.pool_id == r.pool_id for r in msg.routes):
                return list(changes.values())
            token_in = OsmosisNativeToken(msg.tokenIn.denom)
            amount_in = token_in.to_amount(int_amount=str(msg.tokenIn.amount))
            for route in msg.routes:
                token_out = OsmosisNativeToken(route.token_out_denom, self.client.chain_id)
                if route.pool_id == self.pool_id:
                    pool = self
                    changes[amount_in.token] += amount_in  # type: ignore
                else:
                    pool = await GAMMLiquidityPool.new(route.pool_id, self.client)
                amount_in = await pool.get_amount_out_exact_in(amount_in, token_out)
                if route.pool_id == self.pool_id:
                    changes[amount_in.token] -= amount_in  # type: ignore
        if isinstance(msg, MsgSwapExactAmountOut):
            log.debug("Unable to get_reserve_changes_from_msg: MsgSwapExactAmountOut")
            return list(changes.values())  # TODO: implement
        return list(changes.values())
