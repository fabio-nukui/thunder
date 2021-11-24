from __future__ import annotations

import json
from abc import ABC
from decimal import Decimal
from typing import TYPE_CHECKING, Generic, Optional, TypeVar, Union

from terra_sdk.core import AccAddress, Dec
from terra_sdk.core.wasm import MsgExecuteContract

from common.token import DecInput, Token, TokenAmount

if TYPE_CHECKING:
    from .client import CosmosClient

_CW20_WHITELIST_FILE = "resources/addresses/cosmos/{chain_id}/cw20_whitelist.json"

_CW20TokenT = TypeVar("_CW20TokenT", bound="CW20Token")
_CosmosTokenAmountT = TypeVar("_CosmosTokenAmountT", bound="CosmosTokenAmount")


def get_cw20_whitelist(chain_id: str) -> dict[str, AccAddress]:
    with open(_CW20_WHITELIST_FILE.format(chain_id=chain_id)) as f:
        return json.load(f)


class CosmosTokenAmount(TokenAmount):
    token: CosmosToken

    def __init__(
        self,
        token: Token,
        amount: DecInput | Dec = None,
        int_amount: Optional[int | str | Dec] = None,
    ):
        if isinstance(amount, Dec):
            amount = Decimal(str(amount))
        if isinstance(int_amount, Dec):
            int_amount = int(int_amount)
        super().__init__(token, amount, int_amount)

    async def has_allowance(
        self,
        client: CosmosClient,
        spender: AccAddress,
        owner: AccAddress = None,
    ) -> bool:
        if isinstance(self.token, CosmosNativeToken):
            return True
        allowance = await self.token.get_allowance(client, spender, owner)
        return allowance >= self

    def build_msg_increase_allowance(
        self,
        spender: AccAddress,
        owner: AccAddress,
    ) -> MsgExecuteContract:
        assert isinstance(self.token, CW20Token)
        return self.token.build_msg_increase_allowance(spender, owner, self.int_amount)


class BaseCosmosToken(Token, ABC):
    def __lt__(self, other) -> bool:
        if isinstance(other, BaseCosmosToken):
            return self._id < other._id
        return NotImplemented


class CosmosNativeToken(BaseCosmosToken, Generic[_CosmosTokenAmountT]):
    def __init__(self, denom: str, decimals: int, symbol: str = None):
        self.denom = denom
        self.decimals = decimals
        self.symbol = denom[1:].upper() if symbol is None else symbol

    @property
    def _id(self) -> tuple:
        return (self.denom,)

    async def get_balance(
        self,
        client: CosmosClient,
        address: AccAddress = None,
    ) -> _CosmosTokenAmountT:
        return await client.get_balance(self.denom, address)  # type: ignore


class CW20Token(BaseCosmosToken, Generic[_CosmosTokenAmountT]):
    def __init__(self, contract_addr: AccAddress, symbol: str, decimals: int):
        self.contract_addr = contract_addr
        self.symbol = symbol
        self.decimals = decimals

    @property
    def _id(self) -> tuple:
        return (self.contract_addr,)

    @classmethod
    async def from_contract(
        cls: type[_CW20TokenT],
        contract_addr: AccAddress,
        client: CosmosClient,
    ) -> _CW20TokenT:
        res = await client.contract_query(contract_addr, {"token_info": {}})
        return cls(contract_addr, res["symbol"], res["decimals"])

    async def get_minter(self, client: CosmosClient) -> AccAddress | None:
        res = await client.contract_query(self.contract_addr, {"minter": {}})
        if not res:
            return None
        return res["minter"]

    async def get_balance(
        self, client: CosmosClient, address: str = None
    ) -> _CosmosTokenAmountT:
        address = client.address if address is None else address
        res = await client.contract_query(self.contract_addr, {"balance": {"address": address}})
        return self.to_amount(int_amount=res["balance"])

    async def get_supply(self, client: CosmosClient) -> _CosmosTokenAmountT:
        res = await client.contract_query(self.contract_addr, {"token_info": {}})
        return self.to_amount(int_amount=res["total_supply"])

    async def get_allowance(
        self,
        client: CosmosClient,
        spender: AccAddress,
        owner: AccAddress = None,
    ) -> _CosmosTokenAmountT:
        owner = client.address if owner is None else owner
        query = {"allowance": {"owner": owner, "spender": spender}}
        res = await client.contract_query(self.contract_addr, query)
        return self.to_amount(int_amount=res["allowance"])

    def build_msg_increase_allowance(
        self,
        spender: AccAddress,
        owner: AccAddress,
        amount: int | str,
    ) -> MsgExecuteContract:
        execute_msg = {
            "increase_allowance": {
                "spender": spender,
                "amount": str(amount),
            }
        }
        return MsgExecuteContract(
            sender=owner,
            contract=self.contract_addr,
            execute_msg=execute_msg,
        )


CosmosToken = Union[CosmosNativeToken, CW20Token]
