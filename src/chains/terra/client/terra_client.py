from __future__ import annotations

import base64
import json
import logging
import time
from typing import Iterable

from terra_sdk.client.lcd import LCDClient
from terra_sdk.core import Coins
from terra_sdk.key.mnemonic import MnemonicKey

import auth_secrets
import configs
from utils.cache import CacheGroup, ttl_cache

from ..core import BaseTerraClient, TerraTokenAmount
from ..denoms import UST
from .api.market import MarketApi
from .api.oracle import OracleApi
from .api.treasury import TreasuryApi
from .api.tx import TxApi

log = logging.getLogger(__name__)

TERRA_CONTRACT_QUERY_CACHE_SIZE = 10_000
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
        fee_denom: str = UST.denom,
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

        self.market = MarketApi(self)
        self.oracle = OracleApi(self)
        self.treasury = TreasuryApi(self)
        self.tx = TxApi(self)

        gas_prices = self.tx.get_gas_prices() if gas_prices is None else gas_prices
        self.lcd = LCDClient(lcd_uri, chain_id, gas_prices, gas_adjustment)
        self.wallet = self.lcd.wallet(key)
        self.address = self.wallet.key.acc_address

        self.code_ids = _get_code_ids(self.chain_id)
        self.block = self.get_latest_block()

        log.info(f'Initialized {self} at block={self.block}')

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}(lcd_uri={self.lcd_uri}, chain_id={self.chain_id}, '
            f'account={self.key.acc_address})'
        )

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

    @staticmethod
    def encode_msg(msg: dict) -> str:
        bytes_json = json.dumps(msg, separators=(',', ':')).encode('utf-8')
        return base64.b64encode(bytes_json).decode('ascii')

    def get_latest_block(self) -> int:
        return int(self.lcd.tendermint.block_info()['block']['header']['height'])

    def wait_next_block(self) -> Iterable[int]:
        while True:
            new_block = self.get_latest_block()
            block_diff = new_block - self.block
            if block_diff > 0:
                self.block = new_block
                log.debug(f'New block: {self.block}')
                if block_diff > 1:
                    log.warning(f'More than one block passed since last iteration ({block_diff})')
                yield self.block
            time.sleep(configs.TERRA_POLL_INTERVAL)
