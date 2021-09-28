from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Union

from terra_sdk.client.lcd.lcdclient import LCDClient
from terra_sdk.client.lcd.wallet import Wallet
from terra_sdk.core import Coin
from terra_sdk.core.wasm.msgs import MsgExecuteContract
from terra_sdk.key.mnemonic import MnemonicKey

from common import Token, TokenAmount


class TerraNativeToken(Token):
    decimals = 6

    def __init__(self, denom: str):
        self.denom = denom
        self.symbol = 'LUNA' if denom[1:] == 'luna' else denom[1:-1].upper() + 'T'

    @property
    def _id(self) -> tuple:
        return (self.denom, )

    def get_balance(self, client: BaseTerraClient, address: str = None) -> TerraTokenAmount:
        balances = client.get_bank([self.denom], address)
        if not balances:
            return TerraTokenAmount(self, 0)
        return balances[0]


class CW20Token(Token):
    def __init__(self, contract_addr: str, symbol: str, decimals: int):
        self.contract_addr = contract_addr
        self.symbol = symbol
        self.decimals = decimals

    def _id(self) -> tuple:
        return (self.contract_addr, )

    @classmethod
    def from_contract(cls, contract_addr: str, client: BaseTerraClient) -> CW20Token:
        res = client.contract_query(contract_addr, {'token_info': {}})
        return cls(contract_addr, res['symbol'], res['decimals'])

    def get_balance(self, client: BaseTerraClient, address: str = None) -> TerraTokenAmount:
        address = client.address if address is None else address
        res = client.contract_query(self.contract_addr, {'balance': {'address': address}})
        return TerraTokenAmount(self, raw_amount=res['balance'])

    def get_allowance(
        self,
        client: BaseTerraClient,
        spender: str,
        owner: str = None,
    ) -> TerraTokenAmount:
        owner = client.address if owner is None else owner
        query = {'allowance': {'owner': owner, 'spender': spender}}
        res = client.contract_query(self.contract_addr, query)
        return TerraTokenAmount(self, raw_amount=res['allowance'])

    def build_msg_increase_allowance(
        self,
        spender: str,
        owner: str,
        amount: int,
    ) -> MsgExecuteContract:
        execute_msg = {
            'increase_allowance': {
                'spender': spender,
                'amount': amount,
            }
        }
        return MsgExecuteContract(
            sender=owner,
            contract=self.contract_addr,
            execute_msg=execute_msg,
        )


TerraToken = Union[TerraNativeToken, CW20Token]


class TerraTokenAmount(TokenAmount):
    token: TerraToken

    @classmethod
    def from_coin(cls, coin: Coin) -> TerraTokenAmount:
        token = TerraNativeToken(coin.denom)
        return cls(token, raw_amount=coin.amount)

    def to_coin(self) -> Coin:
        assert isinstance(self.token, TerraNativeToken)
        return Coin(self.token.denom, self.raw_amount)

    def has_allowance(self, client: BaseTerraClient, spender: str) -> bool:
        if isinstance(self.token, TerraNativeToken):
            return True
        allowance = self.token.get_allowance(client, spender)
        return allowance >= self

    def build_msg_increase_allowance(self, spender: str, owner: str) -> MsgExecuteContract:
        assert isinstance(self.token, CW20Token)
        return self.token.build_msg_increase_allowance(spender, owner, self.raw_amount)


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
