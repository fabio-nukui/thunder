from __future__ import annotations

import base64
import json
import logging
from decimal import Decimal
from typing import Literal

from terra_sdk.client.lcd import LCDClient
from terra_sdk.core import Coins
from terra_sdk.core.auth import StdTx
from terra_sdk.key.mnemonic import MnemonicKey

import auth_secrets
import configs
import utils
from utils.cache import CacheGroup, ttl_cache

from .core import BaseTerraClient, TerraNativeToken, TerraToken, TerraTokenAmount

log = logging.getLogger(__name__)

TERRA_CONTRACT_QUERY_CACHE_SIZE = 10_000
TERRA_GAS_PRICE_CACHE_TTL = 3600
TERRA_TAX_CACHE_TTL = 7200
DEFAULT_FEE_DENOM = 'uusd'
TERRA_CODE_IDS = 'resources/contracts/terra/{chain_id}/code_ids.json'


def _get_code_ids(chain_id: str) -> dict[str, int]:
    return json.load(open(TERRA_CODE_IDS.format(chain_id=chain_id)))


class TerraClient(BaseTerraClient):
    def __init__(
        self,
        hd_wallet: dict = None,
        lcd_uri: str = configs.TERRA_LCD_URI,
        fcd_uri: str = configs.TERRA_FCD_URI,
        chain_id: str = configs.TERRA_CHAIN_ID,
        fee_denom: str = DEFAULT_FEE_DENOM,
        gas_prices: Coins.Input = None,
        gas_adjustment: float = configs.TERRA_GAS_ADJUSTMENT,
        hd_wallet_index: int = 0,
    ):
        self.lcd_uri = lcd_uri
        self.fcd_uri = fcd_uri
        self.chain_id = chain_id
        self.fee_denom = fee_denom

        hd_wallet = auth_secrets.hd_wallet() if hd_wallet is None else hd_wallet
        key = MnemonicKey(hd_wallet['mnemonic'], hd_wallet['account'], hd_wallet_index)
        self.key = key  # Set key before get_gas_prices() to avoid error with cache debugging

        gas_prices = self.get_gas_prices() if gas_prices is None else gas_prices
        self.lcd = LCDClient(lcd_uri, chain_id, gas_prices, gas_adjustment)
        self.wallet = self.lcd.wallet(key)
        self.address = self.wallet.key.acc_address
        self.code_ids = _get_code_ids(self.chain_id)

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}(lcd_uri={self.lcd_uri}, chain_id={self.chain_id}, '
            f'account={self.key.acc_address})'
        )

    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=TERRA_GAS_PRICE_CACHE_TTL)
    def get_gas_prices(self) -> Coins:
        res = utils.http.get(f'{self.fcd_uri}/v1/txs/gas_prices')
        return Coins(res.json())

    @property
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=TERRA_TAX_CACHE_TTL)
    def tax_rate(self) -> Decimal:
        res = utils.http.get(f'{self.fcd_uri}/treasury/tax_rate')
        return Decimal(res.json()['result'])

    @property
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=TERRA_TAX_CACHE_TTL)
    def tax_caps(self) -> dict[TerraToken, TerraTokenAmount]:
        res = utils.http.get(f'{self.fcd_uri}/treasury/tax_caps')
        caps = {}
        for cap in res.json()['result']:
            token = TerraNativeToken(cap['denom'])
            caps[token] = TerraTokenAmount(token, raw_amount=cap['tax_cap'])
        return caps

    def calculate_tax(
        self,
        amount: TerraTokenAmount,
        payer: Literal['sender'] | Literal['receiver'],
    ) -> TerraTokenAmount:
        if amount.token not in self.tax_caps:
            return TerraTokenAmount(amount.token, 0)
        effective_rate = self.tax_rate if payer == 'sender' else self.tax_rate / (1 + self.tax_rate)
        return min(amount * effective_rate, self.tax_caps[amount.token])

    def deduct_tax(
        self,
        amount: TerraTokenAmount,
        payer: Literal['sender'] | Literal['receiver'] = 'receiver',
    ) -> TerraTokenAmount:
        return amount - self.calculate_tax(amount, payer)

    def estimate_gas_fee(self, tx: StdTx, gas_adjustment: float = None) -> TerraTokenAmount:
        gas_fee = self.lcd.tx.estimate_fee(
            tx,
            gas_adjustment=gas_adjustment,
            fee_denoms=[self.fee_denom],
        )
        return TerraTokenAmount.from_coin(gas_fee.amount.to_list()[0])

    @ttl_cache(CacheGroup.TERRA, TERRA_CONTRACT_QUERY_CACHE_SIZE)
    def contract_query(self, contract_addr: str, query_msg: dict) -> dict:
        return self.lcd.wasm.contract_query(contract_addr, query_msg)

    @ttl_cache(CacheGroup.TERRA, TERRA_CONTRACT_QUERY_CACHE_SIZE)
    def contract_info(self, contract_addr: str) -> dict:
        return self.lcd.wasm.contract_info(contract_addr)

    def get_bank(self, denoms: list[str] = None, address: str = None) -> list[TerraTokenAmount]:
        address = self.address if address is None else address
        coins_balance = self.lcd.bank.balance(address)
        return [
            TerraTokenAmount.from_coin(c)
            for c in coins_balance
            if denoms is None or c.denom in denoms
        ]

    def execute_tx(self, msgs: list[dict]) -> str:
        log.debug(f'Sending tx: {msgs}')
        signed_tx = self.wallet.create_and_sign_tx(msgs, fee_denoms=[self.fee_denom])
        res = self.lcd.tx.broadcast(signed_tx)
        log.debug(f'Tx executed: {res.raw_log}')

        return res.txhash

    @staticmethod
    def encode_msg(msg: dict) -> str:
        return base64.b64encode(json.dumps(msg).encode('utf-8')).decode('ascii')
