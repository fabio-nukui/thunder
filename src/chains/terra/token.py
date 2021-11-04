from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import TYPE_CHECKING, Optional, TypeVar, Union

from terra_sdk.core import AccAddress, Coin, Dec
from terra_sdk.core.wasm import MsgExecuteContract

from common.token import DecInput, Token, TokenAmount

if TYPE_CHECKING:
    from .client import TerraClient


_CW20TokenT = TypeVar("_CW20TokenT", bound="CW20Token")


class TerraTokenAmount(TokenAmount):
    token: TerraToken

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

    @classmethod
    def from_coin(cls, coin: Coin) -> TerraTokenAmount:
        token = TerraNativeToken(coin.denom)
        return cls(token, int_amount=coin.amount)

    def to_coin(self) -> Coin:
        assert isinstance(self.token, TerraNativeToken)
        return Coin(self.token.denom, self.int_amount)

    @classmethod
    def from_str(cls, data: str) -> TerraTokenAmount:
        return cls.from_coin(Coin.from_str(data))

    async def has_allowance(
        self,
        client: "TerraClient",
        spender: AccAddress,
        owner: AccAddress = None,
    ) -> bool:
        if isinstance(self.token, TerraNativeToken):
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


class BaseTerraToken(Token[TerraTokenAmount], ABC):
    amount_class: type[TerraTokenAmount]

    @abstractmethod
    async def get_balance(
        self,
        client: "TerraClient",
        address: AccAddress = None,
    ) -> TerraTokenAmount:
        ...

    def __lt__(self, other) -> bool:
        if isinstance(other, BaseTerraToken):
            return self._id < other._id
        return NotImplemented


class TerraNativeToken(BaseTerraToken):
    amount_class = TerraTokenAmount

    def __init__(self, denom: str):
        self.denom = denom
        self.symbol = "LUNA" if denom == "uluna" else denom[1:-1].upper() + "T"
        if denom[0] == "u":
            self.decimals = 6
        else:
            raise NotImplementedError("TerraNativeToken only implemented for micro (µ) demons")

    @property
    def _id(self) -> tuple:
        return (self.denom,)

    async def get_balance(
        self,
        client: "TerraClient",
        address: AccAddress = None,
    ) -> TerraTokenAmount:
        balances = await client.get_bank([self.denom], address)
        if not balances:
            return self.to_amount(0)
        return balances[0]


class CW20Token(BaseTerraToken):
    amount_class = TerraTokenAmount

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
        client: "TerraClient",
    ) -> _CW20TokenT:
        res = await client.contract_query(contract_addr, {"token_info": {}})
        return cls(contract_addr, res["symbol"], res["decimals"])

    async def get_minter(self, client: "TerraClient") -> AccAddress | None:
        res = await client.contract_query(self.contract_addr, {"minter": {}})
        if not res:
            return None
        return res["minter"]

    async def get_balance(self, client: "TerraClient", address: str = None) -> TerraTokenAmount:
        address = client.address if address is None else address
        res = await client.contract_query(self.contract_addr, {"balance": {"address": address}})
        return self.to_amount(int_amount=res["balance"])

    async def get_supply(self, client: "TerraClient") -> TerraTokenAmount:
        res = await client.contract_query(self.contract_addr, {"token_info": {}})
        return self.to_amount(int_amount=res["total_supply"])

    async def get_allowance(
        self,
        client: "TerraClient",
        spender: AccAddress,
        owner: AccAddress = None,
    ) -> TerraTokenAmount:
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


TerraToken = Union[TerraNativeToken, CW20Token]
