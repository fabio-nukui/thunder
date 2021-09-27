import logging

from web3 import Web3

import utils

from .client import EVMClient
from .core import EVMToken, EVMTokenAmount, NativeToken

log = logging.getLogger(__name__)

ADDR_NATIVE_TOKEN = '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'
ONE_INCH_API_URL = 'https://api.1inch.exchange/v3.0/{chain_id}'
TIMEOUT_REQUESTS = 10.0

DEFAULT_MAX_SLIPPAGE = 0.4  # 0.4% maximum slippage
GAS_ESTIMATE_MARGIN = 1.25  # Estimate gas costs as 125% of simulation


class OneInchExchange:
    def __init__(self, client: EVMClient):
        self.client = client

        self.api_url = f'{ONE_INCH_API_URL.format(chain_id=self.client.chain_id)}'
        self.router_address = self._get_router_address()

    def _get_router_address(self) -> str:
        url = f'{self.api_url}/approve/spender'
        res = utils.http.get(url, timeout=TIMEOUT_REQUESTS)
        return res.json()['address']

    def get_quote(
        self,
        amount_in: EVMTokenAmount,
        token_out: EVMToken,
        gas_price: int = None,
    ) -> EVMTokenAmount:
        token_in = amount_in.token
        address_from = ADDR_NATIVE_TOKEN if isinstance(token_in, NativeToken) else token_in.address
        address_to = ADDR_NATIVE_TOKEN if isinstance(token_out, NativeToken) else token_out.address

        gas_price = self.client.get_gas_price() if gas_price is None else gas_price
        query_params = {
            'fromTokenAddress': address_from,
            'toTokenAddress': address_to,
            'amount': amount_in.raw_amount,
            'gasPrice': gas_price,
        }
        res = utils.http.get(f'{self.api_url}/quote', params=query_params, timeout=TIMEOUT_REQUESTS)
        raw_amount = res.json()['toTokenAmount']
        return EVMTokenAmount(token_out, raw_amount=raw_amount)

    def swap(
        self,
        amount_in: EVMTokenAmount,
        token_out: EVMToken,
        max_slippage: float = DEFAULT_MAX_SLIPPAGE,
        gas_price: int = None,
        infinite_approval: bool = True,
    ) -> str:
        token_in = amount_in.token
        address_from = ADDR_NATIVE_TOKEN if isinstance(token_in, NativeToken) else token_in.address
        address_to = ADDR_NATIVE_TOKEN if isinstance(token_out, NativeToken) else token_out.address

        amount_in.ensure_allowance(self.client, self.router_address, infinite_approval)

        gas_price = self.client.get_gas_price() if gas_price is None else gas_price
        query_params = {
            'fromTokenAddress': address_from,
            'toTokenAddress': address_to,
            'amount': amount_in.raw_amount,
            'fromAddress': self.client.address,
            'slippage': max_slippage,
            'gasPrice': gas_price,
            'allowPartialFill': True,
        }
        res = utils.http.get(
            f'{self.api_url}/swap',
            n_tries=6,
            params=query_params,
            timeout=TIMEOUT_REQUESTS,
        )
        tx = res.json()['tx']
        tx['gas'] = round(tx['gas'] * GAS_ESTIMATE_MARGIN)
        tx['value'] = int(tx['value'])
        tx['gasPrice'] = int(tx['gasPrice'])
        tx['to'] = Web3.toChecksumAddress(tx['to'])

        return self.client.sign_and_send_tx(tx)
