"""Abstract interface classes for terra. Used to avoid circular import errors"""

from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal
from enum import Enum
from typing import Iterator, Type, TypeVar

from terra_sdk.client.lcd import AsyncLCDClient, AsyncWallet
from terra_sdk.core import AccAddress, Coin, Coins
from terra_sdk.core.auth import StdFee
from terra_sdk.core.broadcast import AsyncTxBroadcastResult
from terra_sdk.core.msg import Msg
from terra_sdk.core.wasm import MsgExecuteContract
from terra_sdk.key.mnemonic import MnemonicKey

import utils
from common import AsyncBlockchainClient, Token, TokenAmount

_ITerraTokenAmountT = TypeVar("_ITerraTokenAmountT", bound="ITerraTokenAmount")
_ICW20TokenT = TypeVar("_ICW20TokenT", bound="ICW20Token")


class BaseTerraToken(Token[_ITerraTokenAmountT], ABC):
    amount_class: Type[_ITerraTokenAmountT]

    @abstractmethod
    async def get_balance(
        self,
        client: ITerraClient,
        address: AccAddress = None,
    ) -> _ITerraTokenAmountT:
        ...

    def __lt__(self, other) -> bool:
        if isinstance(other, BaseTerraToken):
            return self._id < other._id
        return NotImplemented


class ITerraNativeToken(BaseTerraToken[_ITerraTokenAmountT], ABC):
    denom: str


class ICW20Token(BaseTerraToken[_ITerraTokenAmountT], ABC):
    contract_addr: AccAddress

    @classmethod
    @abstractmethod
    async def from_contract(
        cls: Type[_ICW20TokenT],
        contract_addr: AccAddress,
        client: ITerraClient,
    ) -> _ICW20TokenT:
        ...

    @abstractmethod
    async def get_supply(self, client: ITerraClient) -> _ITerraTokenAmountT:
        ...

    @abstractmethod
    async def get_allowance(
        self,
        client: ITerraClient,
        spender: AccAddress,
        owner: AccAddress = None,
    ) -> _ITerraTokenAmountT:
        ...

    @abstractmethod
    def build_msg_increase_allowance(
        self,
        spender: AccAddress,
        owner: AccAddress,
        amount: int | str,
    ) -> MsgExecuteContract:
        ...


class ITerraTokenAmount(TokenAmount, ABC):
    token: BaseTerraToken

    @classmethod
    @abstractmethod
    def from_coin(cls, coin: Coin) -> ITerraTokenAmount:
        ...

    @abstractmethod
    def to_coin(self) -> Coin:
        ...

    @abstractmethod
    async def has_allowance(
        self,
        client: ITerraClient,
        spender: AccAddress,
        owner: AccAddress = None,
    ) -> bool:
        ...

    @abstractmethod
    def build_msg_increase_allowance(
        self,
        spender: AccAddress,
        owner: AccAddress,
    ) -> MsgExecuteContract:
        ...


class ITerraClient(AsyncBlockchainClient, ABC):
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

    market: IMarketApi
    mempool: IMempoolApi
    oracle: IOracleApi
    treasury: ITreasuryApi
    tx: ITxApi

    @abstractmethod
    async def contract_query(self, contract_addr: AccAddress, query_msg: dict) -> dict:
        ...

    @abstractmethod
    async def get_bank(
        self,
        denoms: list[str] = None,
        address: AccAddress = None,
    ) -> list[ITerraTokenAmount]:
        ...


class IApi(ABC):
    def __init__(self, client: ITerraClient):
        self.client = client


class IMarketApi(IApi, ABC):
    @abstractmethod
    async def get_amount_out(
        self,
        offer_amount: ITerraTokenAmount,
        ask_denom: ITerraNativeToken,
    ) -> ITerraTokenAmount:
        ...

    @abstractmethod
    async def get_virtual_pools(self) -> tuple[Decimal, Decimal]:
        ...

    @abstractmethod
    async def get_tobin_taxes(self) -> dict[ITerraNativeToken, Decimal]:
        ...

    @abstractmethod
    async def get_market_parameters(self) -> dict[str, Decimal]:
        ...

    @abstractmethod
    async def get_market_parameter(self, param_name: str) -> dict[str, Decimal]:
        ...


class IOracleApi(IApi, ABC):
    @abstractmethod
    async def get_exchange_rates(self) -> dict[ITerraNativeToken, Decimal]:
        ...

    @abstractmethod
    async def get_exchange_rate(
        self,
        from_coin: ITerraNativeToken | str,
        to_coin: ITerraNativeToken | str,
    ) -> Decimal:
        ...


class TaxPayer(str, Enum):
    account = "account"
    contract = "contract"


class ITreasuryApi(IApi, ABC):
    @abstractmethod
    async def get_tax_rate(self) -> Decimal:
        ...

    @abstractmethod
    async def get_tax_caps(self) -> dict[BaseTerraToken, ITerraTokenAmount]:
        ...

    @abstractmethod
    def calculate_tax(
        self,
        amount: ITerraTokenAmount,
        payer: TaxPayer = TaxPayer.contract,
    ) -> ITerraTokenAmount:
        ...

    @abstractmethod
    def deduct_tax(
        self,
        amount: ITerraTokenAmount,
        payer: TaxPayer = TaxPayer.contract,
    ) -> ITerraTokenAmount:
        ...


class ITxApi(IApi, ABC):
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


class IMempoolApi(IApi, ABC):
    @abstractmethod
    async def get_height_mempool(self, height: int) -> tuple[int, dict[str, dict]]:
        ...

    @abstractmethod
    async def loop_height_mempool(self, height: int) -> Iterator[tuple[int, dict[str, dict]]]:
        ...


class IFilter(ABC):
    @abstractmethod
    def match_msgs(self, msgs: list[dict]) -> bool:
        ...
