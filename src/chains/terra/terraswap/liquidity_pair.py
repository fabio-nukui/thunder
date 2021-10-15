from __future__ import annotations

import asyncio
import base64
import json
import logging
import math
from contextlib import asynccontextmanager
from copy import deepcopy
from decimal import Decimal

from terra_sdk.core import AccAddress
from terra_sdk.core.wasm import MsgExecuteContract

from exceptions import InsufficientLiquidity, MaxSpreadAssertion
from utils.cache import CacheGroup, ttl_cache

from ..client import TerraClient
from ..token import CW20Token, TerraNativeToken, TerraToken, TerraTokenAmount
from .utils import token_to_data

log = logging.getLogger(__name__)

FEE = Decimal("0.003")
DEFAULT_MAX_SLIPPAGE_TOLERANCE = Decimal("0.001")
AmountTuple = tuple[TerraTokenAmount, TerraTokenAmount]


class NotTerraswapPair(Exception):
    pass


def _token_amount_to_data(token_amount: TerraTokenAmount) -> dict:
    return {
        "info": token_to_data(token_amount.token),
        "amount": str(token_amount.int_amount),
    }


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
        return await CW20Token.from_contract(contract_addr, client)
    raise TypeError(f"Unexpected data format: {asset_info}")


class LiquidityPair:
    contract_addr: AccAddress
    client: TerraClient
    tokens: tuple[TerraToken, TerraToken]
    lp_token: LPToken
    stop_updates: bool
    _reserves: AmountTuple

    @classmethod
    async def new(
        cls: type[LiquidityPair],
        contract_addr: AccAddress,
        client: TerraClient,
    ) -> LiquidityPair:
        self = super().__new__(cls)
        self.contract_addr = contract_addr
        self.client = client

        self.lp_token = await LPToken.from_pool_contract(self.contract_addr, self.client)
        self.tokens = self.lp_token.pair_tokens

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

    @ttl_cache(CacheGroup.TERRA)
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
        max_spread: Decimal = None,
        belief_price: Decimal = None,
    ) -> TerraTokenAmount:
        amounts = await self.get_swap_amounts(amount_in, safety_margin, max_spread, belief_price)
        return amounts["amounts_out"][1]

    async def get_swap_amounts(
        self,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = False,
        max_spread: Decimal = None,
        belief_price: Decimal = None,
    ) -> dict[str, AmountTuple]:
        """Based on
        https://github.com/terraswap/terraswap/blob/v2.4.1/contracts/terraswap_pair/src/contract.rs#L538  # noqa: E501
        """
        reserve_in, reserve_out = await self._get_in_out_reserves(amount_in=amount_in)

        numerator = reserve_out.amount * amount_in.amount
        denominator = reserve_in.amount + amount_in.amount
        amount_out_before_fees = reserve_out.token.to_amount(numerator / denominator)

        amount_out_before_fees = amount_out_before_fees.safe_margin(safety_margin)
        self._assert_max_spread(
            amount_in.amount,
            amount_out_before_fees.amount,
            reserve_in.amount,
            reserve_out.amount,
            max_spread,
            belief_price,
        )

        fee = amount_out_before_fees * FEE
        amount_out_before_taxes = amount_out_before_fees - fee

        tax = await self.client.treasury.calculate_tax(amount_out_before_taxes)
        amount_out = amount_out_before_taxes - tax

        return {
            "amounts_out": (amount_in * 0, amount_out),
            "fees": (amount_in * 0, fee),
            "taxes": (amount_in * 0, tax),
            "pool_change": (amount_in, -amount_out - tax),
        }

    def _assert_max_spread(
        self,
        amount_in: Decimal,
        amount_out_before_fees: Decimal,
        reserve_in_amount: Decimal,
        reserve_out_amount: Decimal,
        max_spread: Decimal = None,
        belief_price: Decimal = None,
    ):
        """Based on
        https://github.com/terraswap/terraswap/blob/v2.4.1/contracts/terraswap_pair/src/contract.rs#L615  # noqa: E501
        """
        if max_spread is None:
            return
        if belief_price is not None:
            expected_return = amount_in / belief_price
            amount_spread = max(0, expected_return - amount_out_before_fees)
            if (
                amount_out_before_fees < expected_return
                and amount_spread / expected_return > max_spread
            ):
                raise MaxSpreadAssertion
        else:
            amount_out_without_spread = amount_in * reserve_out_amount / reserve_in_amount
            amount_spread = amount_out_without_spread - amount_out_before_fees
            if amount_spread / (amount_out_before_fees + amount_spread) > max_spread:
                raise MaxSpreadAssertion

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

    async def get_reserves_changes_from_msg(self, msg: dict) -> AmountTuple:
        if msg["contract"] == self.contract_addr:
            if "swap" in msg["execute_msg"]:
                swap_msg: dict = msg["execute_msg"]["swap"]
                offer_asset_data = swap_msg["offer_asset"]
                token = await _token_from_data(offer_asset_data["info"], self.client)
                amount_in = token.to_amount(int_amount=offer_asset_data["amount"])
            else:
                raise NotImplementedError(f"Only swap messages implemented, received {msg}")
        else:
            cw20_token_addresses = [
                token.contract_addr for token in self.tokens if isinstance(token, CW20Token)
            ]
            if msg["contract"] in cw20_token_addresses:
                assert "send" in msg["execute_msg"], f"Expected CW20 send, received {msg}"
                terraswap_msg = msg["execute_msg"]["send"]["msg"]
                if "swap" in terraswap_msg:
                    swap_msg = json.loads(base64.b64decode(terraswap_msg["swap"]))
                else:
                    swap_msg = {}
                token = await CW20Token.from_contract(msg["contract"], self.client)
                amount_in = token.to_amount(int_amount=msg["execute_msg"]["send"]["amount"])
            else:
                raise Exception(f"Unexpected msg contract={msg['contract']}")
        max_spread = Decimal(swap_msg["max_spread"]) if "max_spread" in swap_msg else None
        belief_price = Decimal(swap_msg["belief_price"]) if "belief_price" in swap_msg else None
        amounts = await self.get_swap_amounts(
            amount_in,
            max_spread=max_spread,
            belief_price=belief_price,
        )
        amounts_pool_change = amounts["pool_change"]
        if amounts_pool_change[0].token == self.tokens[0]:
            return amounts_pool_change
        return amounts_pool_change[1], amounts_pool_change[0]


class LPToken(CW20Token):
    pair_tokens: tuple[TerraToken, TerraToken]

    @classmethod
    async def from_contract(
        cls,
        contract_addr: AccAddress,
        client: TerraClient,
        pair_tokens: tuple[TerraToken, TerraToken] = None,
    ) -> LPToken:
        self = await super().from_contract(contract_addr, client)
        if pair_tokens is None:
            minter_addr = (await client.contract_query(contract_addr, {"minter": {}}))["minter"]
            pair_data = await client.contract_query(minter_addr, {"pair": {}})
            pair_tokens = await _pair_tokens_from_data(pair_data["asset_infos"], client)
        self.pair_tokens = pair_tokens
        return self

    @classmethod
    async def from_pool_contract(cls, pool_addr: AccAddress, client: TerraClient) -> LPToken:
        pair_data = await client.contract_query(pool_addr, {"pair": {}})
        contract_addr = pair_data["liquidity_token"]
        pair_tokens = await _pair_tokens_from_data(pair_data["asset_infos"], client)
        return await cls.from_contract(contract_addr, client, pair_tokens)

    @property
    def repr_symbol(self):
        return "-".join(t.repr_symbol for t in self.pair_tokens)