from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from decimal import Decimal
from enum import Enum
from typing import Awaitable, Iterator, Type, TypeVar, Union

from terra_sdk.client.lcd import AsyncLCDClient, AsyncWallet
from terra_sdk.core import AccAddress, Coin, Coins
from terra_sdk.core.auth import StdFee
from terra_sdk.core.broadcast import AsyncTxBroadcastResult
from terra_sdk.core.msg import Msg
from terra_sdk.core.wasm import MsgExecuteContract
from terra_sdk.key.mnemonic import MnemonicKey

import utils
from common import BlockchainClient, Token, TokenAmount


class TerraTokenAmount(TokenAmount):
    token: TerraToken

    @classmethod
    def from_coin(cls, coin: Coin) -> TerraTokenAmount:
        token = TerraNativeToken(coin.denom)
        return cls(token, int_amount=coin.amount)

    def to_coin(self) -> Coin:
        assert isinstance(self.token, TerraNativeToken)
        return Coin(self.token.denom, self.int_amount)

    async def has_allowance(
        self,
        client: BaseTerraClient,
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


class BaseTerraToken(Token[TerraTokenAmount]):
    amount_class = TerraTokenAmount

    def __lt__(self, other) -> bool:
        if isinstance(other, BaseTerraToken):
            return self._id < other._id
        return NotImplemented


class TerraNativeToken(BaseTerraToken):
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
        client: BaseTerraClient,
        address: AccAddress = None,
    ) -> TerraTokenAmount:
        balances = await client.get_bank([self.denom], address)
        if not balances:
            return self.to_amount(0)
        return balances[0]


_CW20TokenT = TypeVar("_CW20TokenT", bound="CW20Token")


class CW20Token(BaseTerraToken):
    def __init__(self, contract_addr: AccAddress, symbol: str, decimals: int):
        self.contract_addr = contract_addr
        self.symbol = symbol
        self.decimals = decimals

    @property
    def _id(self) -> tuple:
        return (self.contract_addr,)

    @classmethod
    async def from_contract(
        cls: Type[_CW20TokenT],
        contract_addr: AccAddress,
        client: BaseTerraClient,
    ) -> _CW20TokenT:
        res = await client.contract_query(contract_addr, {"token_info": {}})
        return cls(contract_addr, res["symbol"], res["decimals"])

    async def get_balance(self, client: BaseTerraClient, address: str = None) -> TerraTokenAmount:
        address = client.address if address is None else address
        res = await client.contract_query(self.contract_addr, {"balance": {"address": address}})
        return self.to_amount(int_amount=res["balance"])

    async def get_supply(self, client: BaseTerraClient) -> TerraTokenAmount:
        res = await client.contract_query(self.contract_addr, {"token_info": {}})
        return self.to_amount(int_amount=res["total_supply"])

    async def get_allowance(
        self,
        client: BaseTerraClient,
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
T = TypeVar("T")


class BaseTerraClient(BlockchainClient, ABC):
    loop: asyncio.AbstractEventLoop
    lcd_http_client: utils.ahttp.AsyncClient
    fcd_client: utils.ahttp.AsyncClient
    rpc_http_client: utils.ahttp.AsyncClient
    rpc_websocket_uri: str
    chain_id: str
    key: MnemonicKey
    lcd: AsyncLCDClient
    wallet: AsyncWallet
    address: AccAddress
    code_ids: dict[str, int]
    fee_denom: str
    height: int

    market: BaseMarketApi
    mempool: BaseMempoolApi
    oracle: BaseOracleApi
    treasury: BaseTreasuryApi
    tx: BaseTxApi

    @abstractmethod
    def wait(self, coro: Awaitable[T]) -> T:
        ...

    @abstractmethod
    async def contract_query(self, contract_addr: AccAddress, query_msg: dict) -> dict:
        ...

    @abstractmethod
    async def get_bank(
        self,
        denoms: list[str] = None,
        address: AccAddress = None,
    ) -> list[TerraTokenAmount]:
        ...


class Api:
    def __init__(self, client: BaseTerraClient):
        self.client = client


class BaseMarketApi(Api, ABC):
    @abstractmethod
    async def get_amount_out(
        self,
        offer_amount: TerraTokenAmount,
        ask_denom: TerraNativeToken,
    ) -> TerraTokenAmount:
        ...

    @abstractmethod
    async def get_virtual_pools(self) -> tuple[Decimal, Decimal]:
        ...

    @abstractmethod
    async def get_tobin_taxes(self) -> dict[TerraNativeToken, Decimal]:
        ...

    @abstractmethod
    async def get_market_parameters(self) -> dict[str, Decimal]:
        ...

    @abstractmethod
    async def get_market_parameter(self, param_name: str) -> dict[str, Decimal]:
        ...


class BaseOracleApi(Api, ABC):
    @abstractmethod
    async def get_exchange_rates(self) -> dict[TerraNativeToken, Decimal]:
        ...

    @abstractmethod
    async def get_exchange_rate(
        self,
        from_coin: TerraNativeToken | str,
        to_coin: TerraNativeToken | str,
    ) -> Decimal:
        ...


class TaxPayer(str, Enum):
    account = "account"
    contract = "contract"


class BaseTreasuryApi(Api, ABC):
    @abstractmethod
    async def get_tax_rate(self) -> Decimal:
        ...

    @abstractmethod
    async def get_tax_caps(self) -> dict[TerraToken, TerraTokenAmount]:
        ...

    @abstractmethod
    def calculate_tax(
        self,
        amount: TerraTokenAmount,
        payer: TaxPayer = TaxPayer.contract,
    ) -> TerraTokenAmount:
        ...

    @abstractmethod
    def deduct_tax(
        self,
        amount: TerraTokenAmount,
        payer: TaxPayer = TaxPayer.contract,
    ) -> TerraTokenAmount:
        ...


class BaseTxApi(Api, ABC):
    @abstractmethod
    async def get_gas_prices(self) -> Coins:
        ...

    @abstractmethod
    async def estimate_fee(
        self,
        msgs: list[Msg],
        gas_adjustment: float = None,
    ) -> StdFee:
        ...

    @abstractmethod
    async def execute_msgs(self, msgs: list[Msg], **kwargs) -> AsyncTxBroadcastResult:
        ...


class BaseMempoolApi(Api, ABC):
    @abstractmethod
    async def get_height_mempool(self, height: int) -> tuple[int, dict[str, dict]]:
        ...

    @abstractmethod
    async def loop_height_mempool(self, height: int) -> Iterator[tuple[int, dict[str, dict]]]:
        ...
