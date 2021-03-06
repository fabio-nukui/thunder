from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from typing import Literal, Optional, Union

from eth_account.signers.local import LocalAccount
from web3 import Web3
from web3.contract import ContractFunction
from web3.types import TxParams

import configs
from common import SyncBlockchainClient, Token, TokenAmount

log = logging.getLogger(__name__)

ERC20_ABI = json.load(open("resources/contracts/evm/abis/ERC20.json"))
_NATIVE_TOKENS = {
    configs.ETHEREUM_CHAIN_ID: {"symbol": "ETH", "decimals": "18"},
    configs.BSC_CHAIN_ID: {"symbol": "BNB", "decimals": "18"},
}

# Almost same as max uint256, but uses less gas
INF_APPROVAL_AMOUNT = 0xFF00000000000000000000000000000000000000000000000000000000000000


class EVMTokenAmount(TokenAmount):
    token: EVMToken

    def ensure_allowance(
        self,
        client: BaseEVMClient,
        spender: str,
        infinite_approval: bool = True,
    ):
        if isinstance(self.token, EVMNativeToken):
            return
        allowance = self.token.get_allowance(client.address, spender)
        if allowance < self.int_amount:
            approval_amount = None if infinite_approval else self
            self.token.set_allowance(client, spender, approval_amount)


class EVMNativeToken(Token[EVMTokenAmount]):
    __instances: dict[int, EVMNativeToken] = {}
    amount_class = EVMTokenAmount

    def __new__(
        cls,
        chain_id: int,
        symbol: str = None,
        decimals: int = None,
    ) -> EVMNativeToken:
        if chain_id in cls.__instances:
            return cls.__instances[chain_id]
        self = super().__new__(cls)
        self.__init__(chain_id, symbol, decimals)
        return self

    def __init__(
        self,
        chain_id: int,
        symbol: str = None,
        decimals: int = None,
    ):
        self.chain_id = chain_id
        self.symbol = _NATIVE_TOKENS[chain_id]["symbol"] if symbol is None else symbol
        self.decimals = (
            int(_NATIVE_TOKENS[chain_id]["decimals"]) if decimals is None else decimals
        )

    @property
    def _id(self) -> tuple:
        return (self.chain_id,)


class ERC20Token(Token[EVMTokenAmount]):
    amount_class = EVMTokenAmount

    def __init__(
        self,
        address: str,
        abi: dict = ERC20_ABI,
        client: BaseEVMClient = None,
        chain_id: int = None,
        symbol: str = None,
        decimals: int = None,
    ):
        self.address = Web3.toChecksumAddress(address)
        self.client = client

        if self.client is None:
            self.contract = None
            assert chain_id is not None, "Missing chain_id"
            assert decimals is not None, "Missing decimals"
            assert symbol is not None, "Missing symbol"
        else:
            self.contract = self.client.w3.eth.contract(address=self.address, abi=abi)
            if symbol is None:
                symbol_func = self.contract.functions.symbol()
                symbol = symbol_func.call(block_identifier=self.client.block_identifier)
            if decimals is None:
                decimal_func = self.contract.functions.decimals()
                decimals = decimal_func.call(block_identifier=self.client.block_identifier)
            if chain_id is None:
                chain_id = self.client.chain_id

        self.chain_id = chain_id
        self.symbol = symbol
        self.decimals = decimals

    def __repr__(self):
        return f"{self.__class__.__name__}(symbol={self.symbol}, address={self.address})"

    def get_allowance(self, owner: str, spender: str) -> EVMTokenAmount:
        assert self.contract is not None and self.client is not None
        allowance: int = self.contract.functions.allowance(owner, spender).call(
            block_identifier=self.client.block_identifier
        )
        return EVMTokenAmount(self, int_amount=allowance)

    def set_allowance(
        self,
        client: BaseEVMClient,
        spender: str,
        amount: Optional[int | EVMTokenAmount] = None,
    ) -> str:
        if amount is None:
            amount = INF_APPROVAL_AMOUNT
        elif isinstance(amount, EVMTokenAmount):
            assert self == amount.token
            amount = amount.int_amount

        assert self.contract is not None
        contract_call = self.contract.functions.approve(spender, amount)
        tx_hash = client.sign_and_send_contract_tx(contract_call)
        log.debug(f"Set allowance for {spender} to {amount} ({tx_hash})")
        return tx_hash

    @property
    def _id(self) -> tuple:
        return (self.chain_id, self.address)

    def __lt__(self, other):
        """Use same logic as Uniswap:
        https://github.com/Uniswap/uniswap-sdk-core/blob/main/src/entities/token.ts#L37"""
        if isinstance(other, type(self)):
            assert (
                self.chain_id == other.chain_id
            ), f"Cannot compare tokens in different chains {self.chain_id} / {other.chain_id}"
            return self.address.lower() < other.address.lower()
        return NotImplemented


EVMToken = Union[EVMNativeToken, ERC20Token]


class BaseEVMClient(SyncBlockchainClient, ABC):
    endpoint_uri: str
    chain_id: int
    block_identifier: int | Literal["latest"]
    eip_1559: bool
    gas_multiplier: float
    base_fee_multiplier: float
    w3: Web3
    account: LocalAccount
    address: str

    @abstractmethod
    def get_gas_price(
        self,
        gas_multiplier: float = None,
        base_fee_multiplier: float = None,
        force_legacy_tx: bool = False,
    ) -> dict[str, int]:
        ...

    @abstractmethod
    def sign_and_send_tx(self, tx: TxParams) -> str:
        ...

    @abstractmethod
    def sign_and_send_contract_tx(
        self,
        contract_call: ContractFunction,
        value: int = 0,
        max_gas: int = None,
        gas_multiplier: float = None,
        base_fee_multiplier: float = None,
    ) -> str:
        ...
