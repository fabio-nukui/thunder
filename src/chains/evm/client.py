from __future__ import annotations

import logging
from copy import copy
from typing import Literal

import web3.middleware
from eth_account.datastructures import SignedTransaction
from eth_account.signers.local import LocalAccount
from web3 import Account, HTTPProvider, IPCProvider, Web3, WebsocketProvider
from web3.contract import ContractFunction

import auth_secrets

from .core import DEFAULT_MAX_GAS, BaseEVMClient

log = logging.getLogger(__name__)

Account.enable_unaudited_hdwallet_features()


DEFAULT_CONN_TIMEOUT = 3
MAX_BLOCKS_WAIT_RECEIPT = 10


class EVMClient(BaseEVMClient):
    def __init__(
        self,
        endpoint_uri: str,
        chain_id: int,
        coin_type: int,
        middlewares: list[str] = None,
        hd_wallet: dict = None,
        hd_wallet_index: int = 0,
        timeout: int = None,
        height: int | Literal["latest"] = "latest",
        raise_on_syncing: bool = False,
    ):
        self.endpoint_uri = endpoint_uri
        self.chain_id = chain_id
        self.middlewares = middlewares
        self.timeout = DEFAULT_CONN_TIMEOUT if timeout is None else timeout
        self.height = height

        self.w3 = get_w3(endpoint_uri, middlewares, timeout)

        hd_wallet = auth_secrets.hd_wallet() if hd_wallet is None else hd_wallet

        self.account: LocalAccount = Account.from_mnemonic(
            hd_wallet["mnemonic"],
            account_path=f"m/44'/{coin_type}'/{hd_wallet['account']}'/0/{hd_wallet_index}",
        )
        self.address: str = self.account.address
        super().__init__(raise_on_syncing)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(endpoint_uri={self.endpoint_uri}, address={self.address})"
        )

    @property
    def syncing(self) -> bool:
        return self.w3.eth.syncing

    def get_gas_price(self) -> int:
        return self.w3.eth.gas_price

    def sign_and_send_tx(self, tx: dict) -> str:
        tx = copy(tx)
        tx.setdefault("gas", DEFAULT_MAX_GAS)

        # Avoid dict's setdefault() or get() to avoid side effects / calling expensive functions
        if "nonce" not in tx:
            tx["nonce"] = self.w3.eth.get_transaction_count(self.address)
        tx["gasPrice"] = tx["gasPrice"] if "gasPrice" in tx else self.get_gas_price()

        signed_tx: SignedTransaction = self.account.sign_transaction(tx)
        tx_hash = signed_tx.hash.hex()

        log.debug(f"Sending transaction {tx_hash}: {tx}")
        return self.w3.eth.send_raw_transaction(signed_tx.rawTransaction).hex()

    def sign_and_send_contract_tx(
        self,
        contract_call: ContractFunction,
        value: int = 0,
        gas_price: int = None,
        max_gas: int = DEFAULT_MAX_GAS,
    ) -> str:
        tx = contract_call.buildTransaction(
            {
                "from": self.address,
                "value": value,
                "chainId": self.chain_id,
                "gas": max_gas,
                "gasPrice": self.get_gas_price() if gas_price is None else gas_price,
            }
        )
        return self.sign_and_send_tx(tx)


def get_w3(
    endpoint_uri: str,
    middlewares: list[str] = None,
    timeout: int = DEFAULT_CONN_TIMEOUT,
) -> Web3:
    if endpoint_uri.startswith("http"):
        provider = HTTPProvider(endpoint_uri, request_kwargs={"timeout": timeout})
    elif endpoint_uri.startswith("wss"):
        provider = WebsocketProvider(endpoint_uri, websocket_timeout=timeout)
    elif endpoint_uri.endswith("ipc"):
        provider = IPCProvider(endpoint_uri, timeout)
    else:
        raise ValueError(f"Invalid {endpoint_uri=}")

    if middlewares is None:
        middlewares = []
    else:
        middlewares = [getattr(web3.middleware, m) for m in middlewares if m]

    return Web3(provider, middlewares)
