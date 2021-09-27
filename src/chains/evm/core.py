from __future__ import annotations

import json
from typing import Union

from web3 import Web3

import configs
from common import Token, TokenAmount

from .client import EVMClient

ERC20_ABI = json.load(open('resources/contracts/evm/abis/ERC20.json'))
_NATIVE_TOKENS = {
    configs.ETHEREUM_CHAIN_ID: {'symbol': 'ETH', 'decimals': '18'},
    configs.BSC_CHAIN_ID: {'symbol': 'BNB', 'decimals': '18'},
}


class NativeToken(Token):
    def __init__(
        self,
        chain_id: int,
        symbol: str = None,
        decimals: int = None,
    ):
        self.chain_id = chain_id
        self.symbol = _NATIVE_TOKENS[chain_id]['symbol'] if symbol is None else symbol
        self.decimals = int(_NATIVE_TOKENS[chain_id]['decimals']) if decimals is None else decimals


class ERC20Token(Token):
    def __init__(
        self,
        address: str,
        abi: dict = ERC20_ABI,
        client: EVMClient = None,
        chain_id: int = None,
        symbol: str = None,
        decimals: int = None,
    ):
        self.address = Web3.toChecksumAddress(address)
        self.client = client

        if self.client is None:
            self.contract = None
            assert chain_id is not None, 'Missing chain_id'
            assert decimals is not None, 'Missing decimals'
            assert symbol is not None, 'Missing symbol'
        else:
            self.contract = self.client.w3.eth.contract(address=self.address, abi=abi)
            if symbol is None:
                symbol_func = self.contract.functions.symbol()
                symbol = symbol_func.call(block_identifier=self.client.block)
            if decimals is None:
                decimal_func = self.contract.functions.decimals()
                decimals = decimal_func.call(block_identifier=self.client.block)
            if chain_id is None:
                chain_id = self.client.chain_id

        self.chain_id = chain_id
        self.symbol = symbol
        self.decimals = decimals

    def __repr__(self):
        return f'{self.__class__.__name__}(symbol={self.symbol}, address={self.address})'

    def _id(self) -> tuple:
        return (self.chain_id, self.address)

    def __hash__(self):
        return int(self.address, 16) + self.chain_id

    def __lt__(self, other):
        """Use same logic as Uniswap:
            https://github.com/Uniswap/uniswap-sdk-core/blob/main/src/entities/token.ts#L37"""
        if isinstance(other, type(self)):
            assert self.chain_id == other.chain_id, \
                f'Cannot compare tokens in different chains {self.chain_id} / {other.chain_id}'
            return self.address.lower() < other.address.lower()
        return NotImplemented


EVMToken = Union[NativeToken, ERC20Token]


class EVMTokenAmount(TokenAmount):
    token: EVMToken
