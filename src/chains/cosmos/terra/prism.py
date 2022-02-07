from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import TYPE_CHECKING

from cosmos_sdk.core import AccAddress, Coins
from cosmos_sdk.core.msg import Msg
from cosmos_sdk.core.wasm import MsgExecuteContract

from utils.cache import CacheGroup, ttl_cache

from .native_liquidity_pair import BaseTerraLiquidityPair
from .token import TerraCW20Token, TerraTokenAmount

if TYPE_CHECKING:
    from .client import TerraClient

ADDRESSES_FILE = "resources/addresses/cosmos/{chain_id}/prism.json"
_CONFIG_CACHE_TTL = 60


def _get_addresses(chain_id: str) -> dict[str, AccAddress]:
    return json.load(open(ADDRESSES_FILE.format(chain_id=chain_id)))


class XPrismMinter(BaseTerraLiquidityPair):
    contract_addr: AccAddress
    client: TerraClient
    prism: TerraCW20Token
    xprism: TerraCW20Token
    _exchange_rate: Decimal

    @classmethod
    async def new(cls, client: TerraClient) -> XPrismMinter:
        self = super().__new__(cls)
        self.client = client
        self.contract_addr = _get_addresses(client.chain_id)["prism_gov"]

        config = await self.get_config()
        self.tokens = self.prism, self.xprism = await asyncio.gather(
            TerraCW20Token.from_contract(config["prism_token"], client),
            TerraCW20Token.from_contract(config["xprism_token"], client),
        )

        self._exchange_rate = Decimal(1)
        self.stop_updates = False

        return self

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}"

    async def op_swap(
        self,
        sender: AccAddress,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = True,
        simulate: bool = False,
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        if amount_in.token == self.prism:
            msg = self.get_deposit_msg(sender, amount_in)
        else:
            msg = self.get_withdraw_msg(sender, amount_in)
        amount_out = await self.get_swap_amount_out(amount_in, safety_margin, simulate, msg)
        return amount_out, [msg]

    async def get_exchange_rate(self) -> Decimal:
        if not self.stop_updates:
            state = await self.get_state()
            self._exchange_rate = Decimal(state["exchange_rate"])
        return self._exchange_rate

    @ttl_cache(CacheGroup.TERRA, ttl=_CONFIG_CACHE_TTL)
    async def get_config(self) -> dict:
        res = await self.client.contract_query(self.contract_addr, {"config": {}})
        return res["config"]

    @ttl_cache(CacheGroup.TERRA)
    async def get_state(self) -> dict:
        res = await self.client.contract_query(self.contract_addr, {"xprism_state": {}})
        return res["xprism_state"]

    async def get_swap_amount_out(
        self,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = True,
        simulate: bool = False,
        simulate_msg: Msg = None,
    ) -> TerraTokenAmount:
        if simulate:
            raise NotImplementedError
        exchange_rate = await self.get_exchange_rate()
        if amount_in.token == self.prism:
            amount_out = self.xprism.to_amount(amount_in.amount / exchange_rate)
        elif amount_in.token == self.xprism:
            amount_out = self.prism.to_amount(amount_in.amount * exchange_rate)
        else:
            raise TypeError(f"{amount_in.token=} not PRISM nor xPRISM")
        return amount_out.safe_margin(safety_margin)

    def get_deposit_msg(
        self,
        sender: AccAddress,
        amount_deposit: TerraTokenAmount,
    ) -> MsgExecuteContract:
        return MsgExecuteContract(
            sender=sender,
            contract=self.contract_addr,
            execute_msg={"deposit": {}},
            coins=Coins([amount_deposit.to_coin()]),
        )

    def get_withdraw_msg(
        self,
        sender: AccAddress,
        amount_withdraw: TerraTokenAmount,
    ) -> MsgExecuteContract:
        raise NotImplementedError("xPRISM burn not implemented")

    async def simulate_reserve_change(
        self, amounts: tuple[TerraTokenAmount, TerraTokenAmount]
    ) -> XPrismMinter:
        return self

    async def get_reserve_changes_from_msg(
        self, msg: dict
    ) -> tuple[TerraTokenAmount, TerraTokenAmount]:
        raise NotImplementedError
