from __future__ import annotations

import json
from decimal import Decimal
from typing import TYPE_CHECKING

from cosmos_sdk.core import AccAddress, Coins
from cosmos_sdk.core.wasm import MsgExecuteContract

from ..token import get_cw20_whitelist
from .denoms import UST
from .native_liquidity_pair import BaseTerraLiquidityPair
from .token import TerraCW20Token, TerraTokenAmount

if TYPE_CHECKING:
    from .client import TerraClient

ADDRESSES_FILE = "resources/addresses/cosmos/{chain_id}/anchor.json"


class Market(BaseTerraLiquidityPair):
    contract_addr: AccAddress
    aUST: TerraCW20Token

    @classmethod
    async def new(cls, client: TerraClient):
        self = super().__new__(cls)
        self.client = client

        addresses = json.load(open(ADDRESSES_FILE.format(chain_id=client.chain_id)))
        self.contract_addr = addresses["market"]

        aust_addr = get_cw20_whitelist(client.chain_id)["aUST"]
        self.aUST = await TerraCW20Token.from_contract(aust_addr, client)
        self.tokens = (UST, self.aUST)
        self.stop_updates = False

        return self

    async def get_exchange_rate(self) -> Decimal:
        res = await self.client.contract_query(self.contract_addr, {"epoch_state": {}})
        return Decimal(res["exchange_rate"])

    async def op_swap(
        self,
        sender: AccAddress,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = True,
        simulate: bool = False,
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        if amount_in.token == UST:
            msg = self.get_deposit_msg(sender, amount_in.int_amount)
        else:
            msg = self.get_withdraw_msg(sender, amount_in.int_amount)
        amount_out = await self.get_swap_amount_out(amount_in, safety_margin, simulate, msg)
        return amount_out, [msg]

    async def get_swap_amount_out(
        self,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = False,
        simulate: bool = False,
        simulate_msg: MsgExecuteContract = None,
    ) -> TerraTokenAmount:
        if simulate:
            raise NotImplementedError
        exchange_rate = await self.get_exchange_rate()
        if amount_in.token == UST:
            amount = self.aUST.to_amount(amount_in.amount / exchange_rate)
        elif amount_in.token == self.aUST:
            amount = UST.to_amount(amount_in.amount * exchange_rate)
        else:
            raise TypeError(f"Unexpected {amount_in.token=}")
        return amount.safe_margin(safety_margin)

    def get_deposit_msg(self, sender: AccAddress, amount_deposit: int) -> MsgExecuteContract:
        execute_msg: dict = {"deposit_stable": {}}
        coins = Coins(f"{amount_deposit}{UST.denom}")
        return MsgExecuteContract(sender, self.contract_addr, execute_msg, coins)

    def get_withdraw_msg(self, sender: AccAddress, amount_withdraw: int) -> MsgExecuteContract:
        execute_msg = {
            "send": {
                "msg": "eyJyZWRlZW1fc3RhYmxlIjp7fX0=",  # base64: {"redeem_stable":{}}
                "amount": str(amount_withdraw),
                "contract": self.contract_addr,
            }
        }
        return MsgExecuteContract(sender, self.aUST.contract_addr, execute_msg)

    async def simulate_reserve_change(
        self, amounts: tuple[TerraTokenAmount, TerraTokenAmount]
    ) -> Market:
        return self

    async def get_reserve_changes_from_msg(
        self, msg: dict
    ) -> tuple[TerraTokenAmount, TerraTokenAmount]:
        raise NotImplementedError
