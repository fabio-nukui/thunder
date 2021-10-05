from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal
from enum import Enum
from typing import Type, TypeVar, Union

from terra_sdk.client.lcd.lcdclient import LCDClient
from terra_sdk.client.lcd.wallet import Wallet
from terra_sdk.core import Coin
from terra_sdk.core.auth.data.tx import StdFee
from terra_sdk.core.broadcast import (AsyncTxBroadcastResult, BlockTxBroadcastResult,
                                      SyncTxBroadcastResult)
from terra_sdk.core.coins import Coins
from terra_sdk.core.msg import Msg
from terra_sdk.core.wasm.msgs import MsgExecuteContract
from terra_sdk.key.mnemonic import MnemonicKey

from common import Token, TokenAmount


class TerraTokenAmount(TokenAmount):
    token: TerraToken

    @classmethod
    def from_coin(cls, coin: Coin) -> TerraTokenAmount:
        token = TerraNativeToken(coin.denom)
        return cls(token, int_amount=coin.amount)

    def to_coin(self) -> Coin:
        assert isinstance(self.token, TerraNativeToken)
        return Coin(self.token.denom, self.int_amount)

    def has_allowance(self, client: BaseTerraClient, spender: str, owner: str = None) -> bool:
        if isinstance(self.token, TerraNativeToken):
            return True
        allowance = self.token.get_allowance(client, spender, owner)
        return allowance >= self

    def build_msg_increase_allowance(self, spender: str, owner: str) -> MsgExecuteContract:
        assert isinstance(self.token, CW20Token)
        return self.token.build_msg_increase_allowance(spender, owner, self.int_amount)


class TerraNativeToken(Token[TerraTokenAmount]):
    amount_class = TerraTokenAmount

    def __init__(self, denom: str):
        self.denom = denom
        self.symbol = 'LUNA' if denom == 'uluna' else denom[1:-1].upper() + 'T'
        if denom[0] == 'u':
            self.decimals = 6
        else:
            raise NotImplementedError('TerraNativeToken only implemented for micro (µ) demons')

    @property
    def _id(self) -> tuple:
        return (self.denom, )

    def get_balance(self, client: BaseTerraClient, address: str = None) -> TerraTokenAmount:
        balances = client.get_bank([self.denom], address)
        if not balances:
            return self.to_amount(0)
        return balances[0]


_CW20TokenT = TypeVar('_CW20TokenT', bound='CW20Token')


class CW20Token(Token[TerraTokenAmount]):
    amount_class = TerraTokenAmount

    def __init__(self, contract_addr: str, symbol: str, decimals: int):
        self.contract_addr = contract_addr
        self.symbol = symbol
        self.decimals = decimals

    @property
    def _id(self) -> tuple:
        return (self.contract_addr, )

    @classmethod
    def from_contract(
        cls: Type[_CW20TokenT],
        contract_addr: str,
        client: BaseTerraClient,
    ) -> _CW20TokenT:
        res = client.contract_query(contract_addr, {'token_info': {}})
        return cls(contract_addr, res['symbol'], res['decimals'])

    def get_balance(self, client: BaseTerraClient, address: str = None) -> TerraTokenAmount:
        address = client.address if address is None else address
        res = client.contract_query(self.contract_addr, {'balance': {'address': address}})
        return self.to_amount(int_amount=res['balance'])

    def get_supply(self, client: BaseTerraClient) -> TerraTokenAmount:
        res = client.contract_query(self.contract_addr, {'token_info': {}})
        return self.to_amount(int_amount=res['total_supply'])

    def get_allowance(
        self,
        client: BaseTerraClient,
        spender: str,
        owner: str = None,
    ) -> TerraTokenAmount:
        owner = client.address if owner is None else owner
        query = {'allowance': {'owner': owner, 'spender': spender}}
        res = client.contract_query(self.contract_addr, query)
        return self.to_amount(int_amount=res['allowance'])

    def build_msg_increase_allowance(
        self,
        spender: str,
        owner: str,
        amount: int | str,
    ) -> MsgExecuteContract:
        execute_msg = {
            'increase_allowance': {
                'spender': spender,
                'amount': str(amount),
            }
        }
        return MsgExecuteContract(
            sender=owner,
            contract=self.contract_addr,
            execute_msg=execute_msg,
        )


TerraToken = Union[TerraNativeToken, CW20Token]


class BaseTerraClient(ABC):
    lcd_uri: str
    fcd_uri: str
    chain_id: str
    key: MnemonicKey
    lcd: LCDClient
    wallet: Wallet
    address: str
    code_ids: dict[str, int]
    fee_denom: str
    block: int

    market: BaseMarketApi
    oracle: BaseOracleApi
    treasury: BaseTreasuryApi
    tx: BaseTxApi

    @abstractmethod
    def contract_query(self, contract_addr: str, query_msg: dict) -> dict:
        ...

    @abstractmethod
    def get_bank(self, denoms: list[str] = None, address: str = None) -> list[TerraTokenAmount]:
        ...


class Api:
    def __init__(self, client: BaseTerraClient):
        self.client = client


class BaseMarketApi(Api, ABC):
    pass


class BaseOracleApi(Api, ABC):
    @property
    @abstractmethod
    def exchange_rates(self) -> dict[TerraNativeToken, Decimal]:
        ...

    @abstractmethod
    def get_exchange_rate(
        self,
        from_coin: TerraNativeToken | str,
        to_coin: TerraNativeToken | str,
    ) -> Decimal:
        ...


class TaxPayer(str, Enum):
    account = 'account'
    contract = 'contract'


class BaseTreasuryApi(Api, ABC):
    @property
    @abstractmethod
    def tax_rate(self) -> Decimal:
        ...

    @property
    @abstractmethod
    def tax_caps(self) -> dict[TerraToken, TerraTokenAmount]:
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
    def get_gas_prices(self) -> Coins:
        ...

    @abstractmethod
    def estimate_fee(
        self,
        msgs: list[Msg],
        gas_adjustment: float = None,
    ) -> StdFee:
        ...

    @abstractmethod
    def execute_msgs_block(self, msgs: list[Msg], **kwargs) -> BlockTxBroadcastResult:
        ...

    @abstractmethod
    def execute_msgs_sync(self, msgs: list[Msg], **kwargs) -> SyncTxBroadcastResult:
        ...

    @abstractmethod
    def execute_msgs_async(self, msgs: list[Msg], **kwargs) -> AsyncTxBroadcastResult:
        ...
