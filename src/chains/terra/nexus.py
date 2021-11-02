from __future__ import annotations

from typing import TYPE_CHECKING

from terra_sdk.core import AccAddress
from terra_sdk.core.wasm.msgs import MsgExecuteContract

from .native_liquidity_pair import BaseTerraLiquidityPair
from .token import CW20Token, TerraTokenAmount

if TYPE_CHECKING:
    from .client import TerraClient


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

        if (n_token_minter := await self.n_token.get_minter(client)) != contract_addr:
            raise Exception(f"{n_token_minter=} and {contract_addr=} do not match")

        return self

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.b_token.symbol})"

    async def op_swap(
        self, sender: AccAddress, amount_in: TerraTokenAmount, *args
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        amount_out = await self.get_swap_amount_out(amount_in)
        if amount_in == self.b_token:
            msg = self.get_deposit_msg(sender, amount_in.int_amount)
        else:
            msg = self.get_withdraw_msg(sender, amount_in.int_amount)
        return amount_out, [msg]

    async def get_swap_amount_out(self, amount_in: TerraTokenAmount, *args) -> TerraTokenAmount:
        if amount_in == self.b_token:
            return self.n_token.to_amount(amount_in.amount)
        if amount_in == self.n_token:
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
