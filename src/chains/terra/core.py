from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Union

from terra_sdk.client.lcd.lcdclient import LCDClient
from terra_sdk.client.lcd.wallet import Wallet
from terra_sdk.core import Coin
from terra_sdk.key.mnemonic import MnemonicKey

from common import Token, TokenAmount


class NativeToken(Token):
    decimals = 6

    def __init__(self, denom: str):
        self.denom = denom
        self.symbol = denom[1:].upper()

    @property
    def _id(self) -> tuple:
        return (self.denom, )


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


TerraToken = Union[NativeToken, CW20Token]


class TerraTokenAmount(TokenAmount):
    token: TerraToken

    @classmethod
    def from_coin(cls, coin: Coin) -> TerraTokenAmount:
        token = NativeToken(coin.denom)
        amount = Decimal(str(coin.amount))
        return cls(token, amount)


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
