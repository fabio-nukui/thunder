from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Sequence

from terra_sdk.core import AccAddress
from terra_sdk.core.wasm import MsgExecuteContract

from .native_liquidity_pair import BaseTerraLiquidityPair
from .token import CW20Token, TerraTokenAmount

if TYPE_CHECKING:
    from .client import TerraClient

ADDRESSES_FILE = "resources/addresses/terra/{chain_id}/nexus.json"


def _get_addresses(chain_id: str) -> dict[str, list[dict]]:
    return json.load(open(ADDRESSES_FILE.format(chain_id=chain_id)))


class Factory:
    def __init__(self, client: TerraClient):
        self.client = client
        self.addresses = _get_addresses(client.chain_id)

    async def get_anchor_vaults(self) -> Sequence[AnchorVault]:
        tasks = [
            AnchorVault.new(a["vault_addresss"], a["n_asset_address"], self.client)
            for a in self.addresses["anchor_vaults"]
        ]
        return await asyncio.gather(*tasks)


class AnchorVault(BaseTerraLiquidityPair):
    contract_addr: AccAddress
    client: TerraClient
    b_token: CW20Token
    n_token: CW20Token

    @classmethod
    async def new(
        cls,
        contract_addr: AccAddress,
        n_token_contract_addr: AccAddress,
        client: TerraClient,
    ) -> AnchorVault:
        self = super().__new__(cls)
        self.contract_addr = contract_addr
        self.client = client
        data: dict = await client.contract_query(contract_addr, {"config": {}})

        self.b_token = await CW20Token.from_contract(data["basset_token_addr"], client)
        self.n_token = await CW20Token.from_contract(n_token_contract_addr, client)
        self.tokens = self.b_token, self.n_token
        self._stop_updates = False

        if (n_token_minter := await self.n_token.get_minter(client)) != contract_addr:
            raise Exception(f"{n_token_minter=} and {contract_addr=} do not match")

        return self

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.b_token.symbol})"

    async def op_swap(
        self, sender: AccAddress, amount_in: TerraTokenAmount, *args
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        amount_out = await self.get_swap_amount_out(amount_in)
        if amount_in.token == self.b_token:
            msg = self.get_deposit_msg(sender, amount_in.int_amount)
        else:
            msg = self.get_withdraw_msg(sender, amount_in.int_amount)
        return amount_out, [msg]

    async def get_swap_amount_out(self, amount_in: TerraTokenAmount, *args) -> TerraTokenAmount:
        if amount_in.token == self.b_token:
            return self.n_token.to_amount(amount_in.amount)
        if amount_in.token == self.n_token:
            return self.b_token.to_amount(amount_in.amount)
        raise TypeError(f"{amount_in.token=} not b_asset nor n_asset")

    def get_deposit_msg(self, sender: AccAddress, amount_deposit: int) -> MsgExecuteContract:
        execute_msg = {
            "send": {
                "amount": str(amount_deposit),
                "contract": self.contract_addr,
                "msg": "eyJkZXBvc2l0Ijp7fX0=",  # base64: {"deposit":{}}
            }
        }
        return MsgExecuteContract(sender, self.b_token.contract_addr, execute_msg)

    def get_withdraw_msg(self, sender: AccAddress, amount_withdraw: int) -> MsgExecuteContract:
        execute_msg = {
            "send": {
                "amount": str(amount_withdraw),
                "contract": self.contract_addr,
                "msg": "eyJ3aXRoZHJhdyI6e319",  # base64: {"withdraw":{}}
            }
        }
        return MsgExecuteContract(sender, self.n_token.contract_addr, execute_msg)

    async def simulate_reserve_change(
        self, amounts: tuple[TerraTokenAmount, TerraTokenAmount]
    ) -> AnchorVault:
        return self

    async def get_reserve_changes_from_msg(
        self, msg: dict
    ) -> tuple[TerraTokenAmount, TerraTokenAmount]:
        raise NotImplementedError
