from __future__ import annotations

import asyncio
import logging
from copy import copy
from typing import Literal

import web3.middleware
from eth_account.datastructures import SignedTransaction
from eth_account.signers.local import LocalAccount
from web3 import Account, HTTPProvider, IPCProvider, Web3, WebsocketProvider
from web3._utils.request import _session_cache as web3_http_sessions_cache
from web3.contract import ContractFunction
from web3.types import TxParams

import auth_secrets

from .core import BaseEVMClient

log = logging.getLogger(__name__)

Account.enable_unaudited_hdwallet_features()


DEFAULT_CONN_TIMEOUT = 3
MAX_BLOCKS_WAIT_RECEIPT = 10
DEFAULT_MAX_GAS = 1_000_000
DEFAULT_GAS_MULTIPLIER = 1.01
DEFAULT_BASE_FEE_MULTIPLIER = 2.1


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
        block_identifier: int | Literal["latest"] = "latest",
        eip_1559: bool = True,
        gas_multiplier: float = DEFAULT_GAS_MULTIPLIER,
        base_fee_multiplier: float = DEFAULT_BASE_FEE_MULTIPLIER,
        raise_on_syncing: bool = False,
    ):
        self.endpoint_uri = endpoint_uri
        self.chain_id = chain_id
        self.middlewares = middlewares
        self.timeout = DEFAULT_CONN_TIMEOUT if timeout is None else timeout
        self.block_identifier = block_identifier
        self.eip_1559 = eip_1559
        self.gas_multiplier = gas_multiplier
        self.base_fee_multiplier = base_fee_multiplier

        self.w3 = get_w3(endpoint_uri, middlewares, timeout)
        self.height = self.w3.eth.block_number

        hd_wallet = auth_secrets.hd_wallet() if hd_wallet is None else hd_wallet

        self.account: LocalAccount = Account.from_mnemonic(
            hd_wallet["mnemonic"],
            account_path=f"m/44'/{coin_type}'/{hd_wallet['account']}'/0/{hd_wallet_index}",
        )
        self.address: str = self.account.address
        super().__init__(raise_on_syncing)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"(endpoint_uri={self.endpoint_uri}, address={self.address})"
        )

    @property
    def syncing(self) -> bool:
        if isinstance((syncing := self.w3.eth.syncing), bool):
            return syncing
        return syncing["currentBlock"] >= syncing["highestBlock"]

    def close(self):
        if (
            isinstance(self.w3.provider, WebsocketProvider)
            and self.w3.provider.conn.ws is not None
        ):
            asyncio.get_event_loop().run_until_complete(self.w3.provider.conn.ws.close())
        if isinstance(self.w3.provider, HTTPProvider):
            for session in web3_http_sessions_cache.values():
                session.close()
            web3_http_sessions_cache.clear()

    def get_gas_price(
        self,
        gas_multiplier: float = None,
        base_fee_multiplier: float = None,
        force_legacy_tx: bool = False,
    ) -> dict[str, int]:
        gas_multiplier = gas_multiplier or self.gas_multiplier
        base_fee_multiplier = base_fee_multiplier or self.base_fee_multiplier

        if force_legacy_tx or not self.eip_1559:
            return {"gasPrice": round(int(self.w3.eth.gas_price) * gas_multiplier)}

        base_fee = self.w3.eth.get_block("pending")["baseFeePerGas"]
        return {
            "maxFeePerGas": round(int(base_fee) * base_fee_multiplier),
            "maxPriorityFeePerGas": round(int(self.w3.eth.max_priority_fee) * gas_multiplier),
            "type": 2,
        }

    def sign_and_send_tx(self, tx: TxParams) -> str:
        tx = copy(tx)
        tx.setdefault("gas", DEFAULT_MAX_GAS)

        # Avoid dict's setdefault() or get() to avoid side effects / calling expensive functions
        if "nonce" not in tx:
            tx["nonce"] = self.w3.eth.get_transaction_count(self.address)
        if "gasPrice" not in tx or self.eip_1559 and "maxFeePerGas" not in tx:
            tx.update(self.get_gas_price())  # type: ignore

        signed_tx: SignedTransaction = self.account.sign_transaction(tx)
        tx_hash = signed_tx.hash.hex()

        log.debug(f"Sending transaction {tx_hash}: {tx}")
        return self.w3.eth.send_raw_transaction(signed_tx.rawTransaction).hex()

    def sign_and_send_contract_tx(
        self,
        contract_call: ContractFunction,
        value: int = 0,
        max_gas: int = None,
        gas_multiplier: float = None,
        base_fee_multiplier: float = None,
    ) -> str:
        tx_params = {
            "from": self.address,
            "value": value,
            "chainId": self.chain_id,
            "gas": DEFAULT_MAX_GAS if max_gas is None else max_gas,
            **self.get_gas_price(gas_multiplier, base_fee_multiplier),
        }
        tx = contract_call.buildTransaction(TxParams(**tx_params))
        return self.sign_and_send_tx(tx)


def get_w3(
    endpoint_uri: str,
    middlewares: list[str] = None,
    timeout: int = None,
) -> Web3:
    timeout = DEFAULT_CONN_TIMEOUT if timeout is None else timeout
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
