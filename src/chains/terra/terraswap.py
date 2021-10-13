from __future__ import annotations

import asyncio
import json
import logging
import math
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from copy import deepcopy
from decimal import Decimal
from typing import Type

from terra_sdk.core import AccAddress
from terra_sdk.core.wasm import MsgExecuteContract

from exceptions import InsufficientLiquidity, NotContract
from utils.cache import CacheGroup, ttl_cache

from .client import TerraClient
from .token import CW20Token, TerraNativeToken, TerraToken, TerraTokenAmount

__all__ = [
    "get_addresses",
    "RouteStep",
    "RouteStepNative",
    "RouteStepTerraswap",
    "Router",
    "LiquidityPair",
]

log = logging.getLogger(__name__)

FEE = Decimal("0.003")
TERRASWAP_CODE_ID_KEY = "terraswap_pair"
DEFAULT_MAX_SLIPPAGE_TOLERANCE = Decimal("0.001")
ADDRESSES_FILE = "resources/addresses/terra/{chain_id}/terraswap.json"

AmountTuple = tuple[TerraTokenAmount, TerraTokenAmount]


class NotTerraswapPair(Exception):
    pass


def _token_to_data(token: TerraToken) -> dict[str, dict[str, str]]:
    if isinstance(token, TerraNativeToken):
        return {"native_token": {"denom": token.denom}}
    return {"token": {"contract_addr": token.contract_addr}}


def _token_amount_to_data(token_amount: TerraTokenAmount) -> dict:
    return {
        "info": _token_to_data(token_amount.token),
        "amount": str(token_amount.int_amount),
    }


async def _is_terraswap_pool(contract_addr: AccAddress, client: TerraClient) -> bool:
    try:
        info = await client.contract_info(contract_addr)
    except NotContract:
        return False
    return int(info["code_id"]) == client.code_ids[TERRASWAP_CODE_ID_KEY]


async def _pair_tokens_from_data(
    asset_infos: list[dict],
    client: TerraClient,
) -> tuple[TerraToken, TerraToken]:
    token_0, token_1 = await asyncio.gather(
        _token_from_data(asset_infos[0], client),
        _token_from_data(asset_infos[1], client),
    )
    return token_0, token_1


async def _token_from_data(asset_info: dict, client: TerraClient) -> TerraToken:
    if "native_token" in asset_info:
        return TerraNativeToken(asset_info["native_token"]["denom"])
    if "token" in asset_info:
        contract_addr: AccAddress = asset_info["token"]["contract_addr"]
        try:
            return await LPToken.from_contract(contract_addr, client)
        except NotTerraswapPair:
            return await CW20Token.from_contract(contract_addr, client)
    raise TypeError(f"Unexpected data format: {asset_info}")


def get_addresses(chain_id: str) -> dict:
    return json.load(open(ADDRESSES_FILE.format(chain_id=chain_id)))


class RouteStep(ABC):
    def __init__(
        self,
        token_in: TerraToken,
        token_out: TerraToken,
    ) -> None:
        self.token_in = token_in
        self.token_out = token_out

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(token_in={self.token_in}, token_out={self.token_out})"

    @property
    def sorted_tokens(self) -> tuple[TerraToken, TerraToken]:
        if self.token_in < self.token_out:
            return self.token_in, self.token_out
        return self.token_out, self.token_in

    @abstractmethod
    def to_data(self) -> dict:
        ...


class RouteStepTerraswap(RouteStep):
    def to_data(self) -> dict:
        return {
            "terra_swap": {
                "offer_asset_info": _token_to_data(self.token_in),
                "ask_asset_info": _token_to_data(self.token_out),
            }
        }


class RouteStepNative(RouteStep):
    def __init__(
        self,
        token_in: TerraNativeToken,
        token_out: TerraNativeToken,
    ):
        self.token_in = token_in
        self.token_out = token_out

    def to_data(self) -> dict:
        return {
            "native_swap": {
                "offer_denom": self.token_in.denom,
                "ask_denom": self.token_out.denom,
            }
        }


class Router:
    def __init__(
        self,
        liquidity_pairs: list[LiquidityPair],
        client: TerraClient,
    ):
        self.contract_addr = get_addresses(client.chain_id)["router"]
        self.pairs = {pair.sorted_tokens: pair for pair in liquidity_pairs}
        self.client = client

    async def op_route_swap(
        self,
        sender: AccAddress,
        amount_in: TerraTokenAmount,
        route: list[RouteStep],
        min_amount_out: TerraTokenAmount,
        safety_margin: bool | int = True,
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        assert route, "route cannot be empty"

        swap_operations: list[dict] = []
        next_amount_in = await self.client.treasury.deduct_tax(amount_in)
        for step in route:
            if isinstance(step, RouteStepTerraswap):
                if step.sorted_tokens not in self.pairs:
                    raise Exception(f"No liquidity pair found for {step.sorted_tokens}")
                pair = self.pairs[step.sorted_tokens]
                next_amount_in = await pair.get_swap_amount_out(next_amount_in, safety_margin)
            else:
                assert isinstance(step.token_out, TerraNativeToken)
                next_amount_in = await self.client.market.get_amount_out(
                    next_amount_in, step.token_out, safety_margin
                )
            swap_operations.append(step.to_data())
        amount_out: TerraTokenAmount = next_amount_in

        swap_msg = {
            "execute_swap_operations": {
                "offer_amount": str(amount_in.int_amount),
                "minimum_receive": str(min_amount_out.int_amount),
                "operations": swap_operations,
            }
        }
        if isinstance(amount_in.token, CW20Token):
            contract = amount_in.token.contract_addr
            execute_msg = {
                "send": {
                    "contract": self.contract_addr,
                    "amount": str(amount_in.int_amount),
                    "msg": self.client.encode_msg(swap_msg),
                }
            }
            coins = []
        else:
            contract = self.contract_addr
            execute_msg = swap_msg
            coins = [amount_in.to_coin()]
        msg = MsgExecuteContract(
            sender=sender,
            contract=contract,
            execute_msg=execute_msg,
            coins=coins,
        )
        return amount_out, [msg]


class LiquidityPair:
    contract_addr: AccAddress
    client: TerraClient
    tokens: tuple[TerraToken, TerraToken]
    lp_token: LPToken
    stop_updates: bool
    _reserves: AmountTuple

    @classmethod
    async def new(
        cls: Type[LiquidityPair],
        contract_addr: AccAddress,
        client: TerraClient,
    ) -> LiquidityPair:
        if not await _is_terraswap_pool(contract_addr, client):
            raise NotTerraswapPair

        self = super().__new__(cls)
        self.contract_addr = contract_addr
        self.client = client

        pair_data = await self.client.contract_query(self.contract_addr, {"pair": {}})
        self.tokens = await _pair_tokens_from_data(pair_data["asset_infos"], self.client)
        self.lp_token = await LPToken.from_pool(pair_data["liquidity_token"], self)

        self.stop_updates = False
        self._reserves = self.tokens[0].to_amount(), self.tokens[1].to_amount()

        return self

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.tokens[0].repr_symbol}/{self.tokens[1].repr_symbol})"
        )

    async def get_reserves(self) -> AmountTuple:
        if not self.stop_updates:
            self._reserves = await self._get_reserves()
        return self._reserves

    @ttl_cache(CacheGroup.TERRA, maxsize=1)
    async def _get_reserves(self) -> AmountTuple:
        data = await self.client.contract_query(self.contract_addr, {"pool": {}})
        return (
            self.tokens[0].to_amount(int_amount=data["assets"][0]["amount"]),
            self.tokens[1].to_amount(int_amount=data["assets"][1]["amount"]),
        )

    @property
    def sorted_tokens(self) -> tuple[TerraToken, TerraToken]:
        if self.tokens[0] < self.tokens[1]:
            return self.tokens[0], self.tokens[1]
        return self.tokens[1], self.tokens[0]

    @asynccontextmanager
    async def simulate_reserve_change(self, amounts: AmountTuple):
        amounts = self._fix_amounts_order(amounts)
        reserves = deepcopy(await self.get_reserves())
        stop_updates = self.stop_updates
        try:
            self.stop_updates = True
            self._reserves = reserves[0] + amounts[0], reserves[1] + amounts[1]
            yield
        finally:
            self._reserves = reserves
            self.stop_updates = stop_updates

    def _fix_amounts_order(self, amounts: AmountTuple) -> AmountTuple:
        if (amounts[1].token, amounts[0].token) == self.tokens:
            return amounts[1], amounts[0]
        if (amounts[0].token, amounts[1].token) == self.tokens:
            return amounts
        raise Exception("Tokens in amounts do not match reserves")

    async def get_price(self, token_quote: TerraNativeToken) -> Decimal:
        if token_quote in self.tokens:
            reference_token = token_quote
            exchange_rate = Decimal(1)
        else:
            for token in self.tokens:
                if isinstance(token, TerraNativeToken):
                    reference_token = token
                    break
            else:
                raise NotImplementedError("not implemented for pools without a native token")
            exchange_rate = await self.client.oracle.get_exchange_rate(reference_token, token_quote)
        supply = await self.lp_token.get_supply(self.client)
        for reserve in await self.get_reserves():
            if reserve.token == reference_token:
                amount_per_lp_token = reserve.amount / supply.amount
                return amount_per_lp_token * exchange_rate * 2
        raise Exception  # Should never reach

    async def op_swap(
        self,
        sender: AccAddress,
        amount_in: TerraTokenAmount,
        max_slippage: Decimal = DEFAULT_MAX_SLIPPAGE_TOLERANCE,
        safety_margin: bool | int = True,
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        amount_out = await self.get_swap_amount_out(amount_in, safety_margin)
        min_amount_out = (amount_out * (1 - max_slippage)).safe_margin(safety_margin)
        msg = self.build_swap_msg(sender, amount_in, min_amount_out)

        return amount_out, [msg]

    async def get_swap_amount_out(
        self,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = False,
    ) -> TerraTokenAmount:
        return (await self.get_swap_amounts(amount_in, safety_margin))["amounts_out"][1]

    async def get_swap_amounts(
        self,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = False,
    ) -> dict[str, AmountTuple]:
        reserve_in, reserve_out = await self._get_in_out_reserves(amount_in=amount_in)

        numerator = reserve_out.amount * amount_in.amount
        denominator = reserve_in.amount + amount_in.amount
        amount_out = reserve_out.token.to_amount(numerator / denominator)

        amount_out = amount_out.safe_margin(safety_margin)
        amount_out = amount_out - (fee := amount_out * FEE)
        amount_out = amount_out - (tax := await self.client.treasury.calculate_tax(amount_out))

        return {
            "amounts_out": (amount_in * 0, amount_out),
            "fees": (amount_in * 0, fee),
            "taxes": (amount_in * 0, tax),
            "pool_change": (amount_in, -amount_out - tax),
        }

    async def _get_in_out_reserves(
        self,
        amount_in: TerraTokenAmount = None,
        amount_out: TerraTokenAmount = None,
    ) -> AmountTuple:
        """Given an amount in and/or an amount out, checks for insuficient liquidity and return
        the reserves pair in order reserve_in, reserve_out"""
        assert amount_in is None or amount_in.token in self.tokens, "amount_in not in pair"
        assert amount_out is None or amount_out.token in self.tokens, "amount_out not in pair"

        reserves = await self.get_reserves()
        if reserves[0] == 0 or reserves[1] == 0:
            raise InsufficientLiquidity
        if amount_in is not None:
            token_in = amount_in.token
        elif amount_out is not None:
            token_in = self.tokens[0] if amount_out.token == self.tokens[1] else self.tokens[1]
        else:
            raise Exception("At least one of token_in or token_out must be passed")

        if token_in == self.tokens[0]:
            reserve_in, reserve_out = reserves
        else:
            reserve_out, reserve_in = reserves
        if amount_out is not None and amount_out >= reserve_out:
            raise InsufficientLiquidity
        return reserve_in, reserve_out

    def build_swap_msg(
        self,
        sender: AccAddress,
        amount_in: TerraTokenAmount,
        min_out: TerraTokenAmount,
    ) -> MsgExecuteContract:
        belief_price = amount_in.amount / min_out.amount
        swap_msg = {"belief_price": f"{belief_price:.18f}", "max_spread": "0.0"}
        if isinstance(token_in := amount_in.token, CW20Token):
            contract = token_in.contract_addr
            execute_msg = {
                "send": {
                    "contract": self.contract_addr,
                    "amount": str(amount_in.int_amount),
                    "msg": TerraClient.encode_msg({"swap": swap_msg}),
                }
            }
            coins = []
        else:
            contract = self.contract_addr
            execute_msg = {"swap": {"offer_asset": _token_amount_to_data(amount_in), **swap_msg}}
            coins = [amount_in.to_coin()]

        return MsgExecuteContract(
            sender=sender, contract=contract, execute_msg=execute_msg, coins=coins
        )

    async def op_remove_single_side(
        self,
        sender: AccAddress,
        amount_burn: TerraTokenAmount,
        token_out: TerraToken,
        max_slippage: Decimal = DEFAULT_MAX_SLIPPAGE_TOLERANCE,
        safety_margin: bool | int = True,
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        assert token_out in self.tokens
        amounts = await self.get_remove_liquidity_amounts(amount_burn, safety_margin)
        msg_remove_liquidity = self.build_remove_liquidity_msg(sender, amount_burn)
        if token_out == self.tokens[0]:
            amount_keep, amount_swap = amounts["amounts_out"]
        else:
            amount_swap, amount_keep = amounts["amounts_out"]
        async with self.simulate_reserve_change(amounts["pool_change"]):
            amount_out, msgs_swap = await self.op_swap(
                sender, amount_swap, max_slippage, safety_margin
            )
        return amount_keep + amount_out, [msg_remove_liquidity] + msgs_swap

    async def get_remove_liquidity_amounts_out(
        self,
        amount_burn: TerraTokenAmount,
        safety_margin: bool | int = False,
    ) -> AmountTuple:
        return (await self.get_remove_liquidity_amounts(amount_burn, safety_margin))["amounts_out"]

    async def get_remove_liquidity_amounts(
        self,
        amount_burn: TerraTokenAmount,
        safety_margin: bool | int = False,
    ) -> dict[str, AmountTuple]:
        assert amount_burn.token == self.lp_token

        reserves = await self.get_reserves()
        total_supply = await self.lp_token.get_supply(self.client)
        share = amount_burn / total_supply
        amounts = reserves[0] * share, reserves[1] * share

        amounts = amounts[0].safe_margin(safety_margin), amounts[1].safe_margin(safety_margin)
        taxes = (
            await self.client.treasury.calculate_tax(amounts[0]),
            await self.client.treasury.calculate_tax(amounts[1]),
        )
        amounts_out = amounts[0] - taxes[0], amounts[1] - taxes[1]
        return {
            "amounts_out": amounts_out,
            "taxes": taxes,
            "pool_change": (-amounts[0], -amounts[1]),
        }

    def build_remove_liquidity_msg(
        self,
        sender: AccAddress,
        amount_burn: TerraTokenAmount,
    ) -> MsgExecuteContract:
        assert amount_burn.token == self.lp_token
        execute_msg = {
            "send": {
                "amount": str(amount_burn.int_amount),
                "contract": self.contract_addr,
                "msg": TerraClient.encode_msg({"withdraw_liquidity": {}}),
            }
        }
        return MsgExecuteContract(
            sender=sender,
            contract=self.lp_token.contract_addr,
            execute_msg=execute_msg,
        )

    async def op_remove_liquidity(
        self,
        sender: AccAddress,
        amount_burn: TerraTokenAmount,
        safety_margin: bool | int = True,
    ) -> tuple[AmountTuple, list[MsgExecuteContract]]:
        amounts = await self.get_remove_liquidity_amounts_out(amount_burn, safety_margin)
        msg_remove_liquidity = self.build_remove_liquidity_msg(sender, amount_burn)
        return amounts, [msg_remove_liquidity]

    async def op_add_single_side(
        self,
        sender: AccAddress,
        amount_in: TerraTokenAmount,
        slippage_tolerance: Decimal = DEFAULT_MAX_SLIPPAGE_TOLERANCE,
        safety_margin: bool | int = True,
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        reserve_in, reserve_out = await self._get_in_out_reserves(amount_in)

        # Calculate optimum ratio to swap before adding liquidity, excluding tax influence
        aux = FEE * (reserve_in.amount + amount_in.amount) - 2 * reserve_in.amount
        numerator = Decimal(math.sqrt(aux ** 2 + 4 * reserve_in.amount * amount_in.amount)) + aux
        denominator = 2 * amount_in.amount
        ratio_swap = numerator / denominator

        amount_in_swap = amount_in * ratio_swap
        amounts_swap = await self.get_swap_amounts(amount_in_swap)

        if (tax := amounts_swap["taxes"][1]) > 0:
            amount_in_swap += reserve_in * (tax / reserve_out / 2)
            amounts_swap = await self.get_swap_amounts(amount_in_swap)

        min_amount_out = amounts_swap["amounts_out"][1] * (1 - slippage_tolerance)
        min_amount_out = min_amount_out.safe_margin(safety_margin)
        msg_swap = self.build_swap_msg(sender, amount_in_swap, min_amount_out)

        amount_in_keep = amount_in - amount_in_swap
        amounts_add = (
            amount_in_keep.safe_margin(safety_margin),
            amounts_swap["amounts_out"][1].safe_margin(safety_margin),
        )

        async with self.simulate_reserve_change(amounts_swap["pool_change"]):
            amount_out, msgs_add_liquidity = await self.op_add_liquidity(
                sender, amounts_add, slippage_tolerance, safety_margin
            )
        return amount_out, [msg_swap] + msgs_add_liquidity

    async def op_add_liquidity(
        self,
        sender: AccAddress,
        amounts_in: AmountTuple,
        slippage_tolerance: Decimal = DEFAULT_MAX_SLIPPAGE_TOLERANCE,
        safety_margin: bool | int = True,
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        amount_out = await self.get_add_liquidity_amount_out(
            amounts_in, slippage_tolerance, safety_margin
        )
        msgs = await self.build_add_liquity_msgs(sender, amounts_in, slippage_tolerance)
        return amount_out, msgs

    async def get_add_liquidity_amount_out(
        self,
        amounts_in: AmountTuple,
        slippage_tolerance: Decimal = DEFAULT_MAX_SLIPPAGE_TOLERANCE,
        safety_margin: bool | int = False,
    ) -> TerraTokenAmount:
        reserves = await self.get_reserves()
        amounts_in = await self._check_amounts_add_liquidity(amounts_in, slippage_tolerance)
        add_ratio = min(amounts_in[0] / reserves[0], amounts_in[1] / reserves[1])
        amount = await self.lp_token.get_supply(self.client) * add_ratio

        return amount.safe_margin(safety_margin)

    async def _check_amounts_add_liquidity(
        self,
        amounts_in: AmountTuple,
        slippage_tolerance: Decimal = DEFAULT_MAX_SLIPPAGE_TOLERANCE,
    ) -> AmountTuple:
        reserves = await self.get_reserves()
        amounts_in = self._fix_amounts_order(amounts_in)
        amounts_ratio = amounts_in[0].amount / amounts_in[1].amount
        current_ratio = reserves[0].amount / reserves[1].amount
        assert abs(amounts_ratio / current_ratio - 1) < slippage_tolerance
        return amounts_in

    async def build_add_liquity_msgs(
        self,
        sender: AccAddress,
        amounts_in: AmountTuple,
        slippage_tolerance: Decimal = DEFAULT_MAX_SLIPPAGE_TOLERANCE,
    ) -> list[MsgExecuteContract]:
        msgs = []
        for amount in amounts_in:
            if not await amount.has_allowance(self.client, self.contract_addr, sender):
                msgs.append(amount.build_msg_increase_allowance(self.contract_addr, sender))
        execute_msg = {
            "provide_liquidity": {
                "assets": [
                    _token_amount_to_data(amounts_in[0]),
                    _token_amount_to_data(amounts_in[1]),
                ],
                "slippage_tolerance": str(round(slippage_tolerance, 18)),
            }
        }
        coins = [
            amount.to_coin() for amount in amounts_in if isinstance(amount.token, TerraNativeToken)
        ]
        msgs.append(
            MsgExecuteContract(
                sender=sender,
                contract=self.contract_addr,
                execute_msg=execute_msg,
                coins=coins,
            )
        )
        return msgs


class LPToken(CW20Token):
    pair_tokens: tuple[TerraToken, TerraToken]

    @classmethod
    async def from_pool(cls, contract_addr: AccAddress, pool: LiquidityPair) -> LPToken:
        self = await super().from_contract(contract_addr, pool.client)
        self.pair_tokens = pool.tokens
        return self

    @classmethod
    async def from_contract(cls, contract_addr: AccAddress, client: TerraClient) -> LPToken:
        minter_addr = (await client.contract_query(contract_addr, {"minter": {}}))["minter"]
        return (await LiquidityPair.new(minter_addr, client)).lp_token

    @property
    def repr_symbol(self):
        return "-".join(t.repr_symbol for t in self.pair_tokens)
