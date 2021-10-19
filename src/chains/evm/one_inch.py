from __future__ import annotations

import logging
from typing import Any

from web3 import Web3

import utils

from .client import EVMClient
from .core import EVMNativeToken, EVMToken, EVMTokenAmount

log = logging.getLogger(__name__)

NATIVE_ADDRESS = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
ONE_INCH_API_URL = "https://api.1inch.exchange/v3.0/{chain_id}"
TIMEOUT_REQUESTS = 10.0

DEFAULT_MAX_SLIPPAGE = 0.4  # 0.4% maximum slippage
GAS_ESTIMATE_MARGIN = 1.25  # Estimate gas costs as 125% of simulation


class OneInchExchange:
    def __init__(self, client: EVMClient):
        self.client = client

        self.api_url = f"{ONE_INCH_API_URL.format(chain_id=self.client.chain_id)}"
        self.router_address = self._get_router_address()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.client.__class__.__name__})"

    def _get_router_address(self) -> str:
        url = f"{self.api_url}/approve/spender"
        res = utils.http.get(url, timeout=TIMEOUT_REQUESTS)
        return res.json()["address"]

    def get_quote(
        self,
        amount_in: EVMTokenAmount,
        token_out: EVMToken,
        gas_multiplier: float = None,
    ) -> EVMTokenAmount:
        token_in = amount_in.token
        address_from = NATIVE_ADDRESS if isinstance(token_in, EVMNativeToken) else token_in.address
        address_to = NATIVE_ADDRESS if isinstance(token_out, EVMNativeToken) else token_out.address

        query_params = {
            "fromTokenAddress": address_from,
            "toTokenAddress": address_to,
            "amount": amount_in.int_amount,
            **self.client.get_gas_price(gas_multiplier, force_legacy_tx=True),
        }
        res = utils.http.get(f"{self.api_url}/quote", params=query_params, timeout=TIMEOUT_REQUESTS)
        int_amount = res.json()["toTokenAmount"]
        return EVMTokenAmount(token_out, int_amount=int_amount)

    def swap(
        self,
        amount_in: EVMTokenAmount,
        token_out: EVMToken,
        max_slippage: float = DEFAULT_MAX_SLIPPAGE,
        gas_multiplier: float = None,
        base_fee_multiplier: float = None,
        infinite_approval: bool = True,
    ) -> str:
        token_in = amount_in.token
        address_from = NATIVE_ADDRESS if isinstance(token_in, EVMNativeToken) else token_in.address
        address_to = NATIVE_ADDRESS if isinstance(token_out, EVMNativeToken) else token_out.address

        amount_in.ensure_allowance(self.client, self.router_address, infinite_approval)

        query_params = {
            "fromTokenAddress": address_from,
            "toTokenAddress": address_to,
            "amount": amount_in.int_amount,
            "fromAddress": self.client.address,
            "slippage": max_slippage,
            **self.client.get_gas_price(gas_multiplier, force_legacy_tx=True),
            "allowPartialFill": True,
        }
        res = utils.http.get(
            f"{self.api_url}/swap",
            n_tries=6,
            params=query_params,
            timeout=TIMEOUT_REQUESTS,
        )
        tx: dict[str, Any] = res.json()["tx"]
        tx["gas"] = round(tx["gas"] * GAS_ESTIMATE_MARGIN)
        tx["value"] = int(tx["value"])
        tx.update(self.client.get_gas_price(gas_multiplier, base_fee_multiplier))
        tx["to"] = Web3.toChecksumAddress(tx["to"])

        return self.client.sign_and_send_tx(tx)
