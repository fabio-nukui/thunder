from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Union

from terra_sdk.client.lcd.lcdclient import LCDClient
from terra_sdk.client.lcd.wallet import Wallet
from terra_sdk.core import Coin
from terra_sdk.key.mnemonic import MnemonicKey

from common import Token, TokenAmount


class TerraNativeToken(Token):
    decimals = 6

    def __init__(self, denom: str):
        self.denom = denom
        self.symbol = denom[1:].upper()

    @property
    def _id(self) -> tuple:
        return (self.denom, )

    def get_balance(self, client: BaseTerraClient, address: str = None) -> TerraTokenAmount:
        return client.get_bank([self.denom], address)[0]


class CW20Token(Token):
    def __init__(self, contract_addr: str, symbol: str, decimals: int):
        self.contract_addr = contract_addr
        self.symbol = symbol
        self.decimals = decimals

    def _id(self) -> tuple:
        return (self.contract_addr, )

    @classmethod
    def from_contract(cls, contract_addr: str, client: BaseTerraClient) -> CW20Token:
        msg = client.contract_query(contract_addr, {'token_info': {}})
        return cls(contract_addr, msg['symbol'], msg['decimals'])

    def get_balance(self, client: BaseTerraClient, address: str = None) -> TerraTokenAmount:
        address = client.address if address is None else address
        msg = client.contract_query(self.contract_addr, {'balance': {'address': address}})
        return TerraTokenAmount(self, raw_amount=msg['balance'])


TerraToken = Union[TerraNativeToken, CW20Token]


class TerraTokenAmount(TokenAmount):
    token: TerraToken

    @classmethod
    def from_coin(cls, coin: Coin) -> TerraTokenAmount:
        token = TerraNativeToken(coin.denom)
        return cls(token, raw_amount=coin.amount)


class BaseTerraClient(ABC):
    lcd_uri: str
    fcd_uri: str
    chain_id: str
    key: MnemonicKey
    lcd: LCDClient
    wallet: Wallet
    address: str

    @abstractmethod
    def contract_query(self, contract_addr: str, query_msg: dict) -> dict:
        ...

    @abstractmethod
    def get_bank(self, denoms: list[str] = None, address: str = None) -> list[TerraTokenAmount]:
        ...
