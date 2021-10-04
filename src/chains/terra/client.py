from __future__ import annotations

import base64
import json
import logging
import time
from decimal import Decimal
from enum import Enum
from typing import Callable, Iterable, TypeVar

from terra_sdk.client.lcd import LCDClient
from terra_sdk.core import Coins
from terra_sdk.core.auth import StdFee
from terra_sdk.core.auth.data.tx import StdTx
from terra_sdk.core.broadcast import (AsyncTxBroadcastResult, BlockTxBroadcastResult,
                                      SyncTxBroadcastResult)
from terra_sdk.core.msg import Msg
from terra_sdk.key.mnemonic import MnemonicKey

import auth_secrets
import configs
import utils
from utils.cache import CacheGroup, ttl_cache

from .core import LUNA, UST, BaseTerraClient, TerraNativeToken, TerraToken, TerraTokenAmount

log = logging.getLogger(__name__)

TERRA_CONTRACT_QUERY_CACHE_SIZE = 10_000
TERRA_GAS_PRICE_CACHE_TTL = 3600
TERRA_TAX_CACHE_TTL = 7200
TERRA_CODE_IDS = 'resources/contracts/terra/{chain_id}/code_ids.json'
MAX_PRECISION = 18


def _get_code_ids(chain_id: str) -> dict[str, int]:
    return json.load(open(TERRA_CODE_IDS.format(chain_id=chain_id)))


class TaxPayer(str, Enum):
    account = 'account'
    contract = 'contract'


_BroadcastResutT = TypeVar(
    '_BroadcastResutT',
    BlockTxBroadcastResult,
    SyncTxBroadcastResult,
    AsyncTxBroadcastResult,
)


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

        gas_prices = self.get_gas_prices() if gas_prices is None else gas_prices
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

    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=TERRA_GAS_PRICE_CACHE_TTL)
    def get_gas_prices(self) -> Coins:
        res = utils.http.get(f'{self.fcd_uri}/v1/txs/gas_prices')
        return Coins(res.json())

    @property
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=TERRA_TAX_CACHE_TTL)
    def tax_rate(self) -> Decimal:
        return Decimal(str(self.lcd.treasury.tax_rate()))

    @property
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=TERRA_TAX_CACHE_TTL)
    def tax_caps(self) -> dict[TerraToken, TerraTokenAmount]:
        res = utils.http.get(f'{self.lcd_uri}/terra/treasury/v1beta1/tax_caps')
        caps = {}
        for cap in res.json()['tax_caps']:
            token = TerraNativeToken(cap['denom'])
            caps[token] = TerraTokenAmount(token, int_amount=cap['tax_cap'])
        return caps

    def calculate_tax(
        self,
        amount: TerraTokenAmount,
        payer: TaxPayer = TaxPayer.contract,
    ) -> TerraTokenAmount:
        if amount.token not in self.tax_caps:
            return TerraTokenAmount(amount.token, 0)
        if payer == TaxPayer.account:
            effective_rate = self.tax_rate
        else:
            effective_rate = self.tax_rate / (1 + self.tax_rate)
        return min(amount * effective_rate, self.tax_caps[amount.token])

    def deduct_tax(
        self,
        amount: TerraTokenAmount,
        payer: TaxPayer = TaxPayer.contract,
    ) -> TerraTokenAmount:
        return amount - self.calculate_tax(amount, payer)

    def estimate_fee(
        self,
        msgs: list[Msg],
        gas_adjustment: float = None,
    ) -> StdFee:
        return self.lcd.tx.estimate_fee(
            self.address,
            msgs,
            gas_adjustment=gas_adjustment,
            fee_denoms=[self.fee_denom],
        )

    @ttl_cache(CacheGroup.TERRA, TERRA_CONTRACT_QUERY_CACHE_SIZE)
    def contract_query(self, contract_addr: str, query_msg: dict) -> dict:
        return self.lcd.wasm.contract_query(contract_addr, query_msg)

    @ttl_cache(CacheGroup.TERRA, TERRA_CONTRACT_QUERY_CACHE_SIZE)
    def contract_info(self, contract_addr: str) -> dict:
        return self.lcd.wasm.contract_info(contract_addr)

    @property
    @ttl_cache(CacheGroup.TERRA, maxsize=1)
    def exchange_rates(self) -> dict[TerraNativeToken, Decimal]:
        rates = {
            TerraNativeToken(c.denom): Decimal(str(c.amount))
            for c in self.lcd.oracle.exchange_rates().to_list()
        }
        rates[LUNA] = Decimal(1)
        return rates

    def get_exchange_rate(
        self,
        from_coin: TerraNativeToken | str,
        to_coin: TerraNativeToken | str,
    ) -> Decimal:
        if isinstance(from_coin, str):
            from_coin = TerraNativeToken(from_coin)
        if isinstance(to_coin, str):
            to_coin = TerraNativeToken(to_coin)
        return round(self.exchange_rates[to_coin] / self.exchange_rates[from_coin], MAX_PRECISION)

    def get_bank(self, denoms: list[str] = None, address: str = None) -> list[TerraTokenAmount]:
        address = self.address if address is None else address
        coins_balance = self.lcd.bank.balance(address)
        return [
            TerraTokenAmount.from_coin(c)
            for c in coins_balance
            if denoms is None or c.denom in denoms
        ]

    def _execute_msgs(
        self,
        msgs: list,
        broadcast_func: Callable[[StdTx], _BroadcastResutT],
        **kwargs,
    ) -> _BroadcastResutT:
        log.debug(f'Sending tx: {msgs}')
        signed_tx = self.wallet.create_and_sign_tx(msgs, fee_denoms=[self.fee_denom], **kwargs)

        res = broadcast_func(signed_tx)
        log.debug(f'Tx executed: {res.txhash}')
        return res

    def execute_msgs_block(self, msgs: list[Msg], **kwargs) -> BlockTxBroadcastResult:
        return self._execute_msgs(msgs, broadcast_func=self.lcd.tx.broadcast, **kwargs)

    def execute_msgs_sync(self, msgs: list[Msg], **kwargs) -> SyncTxBroadcastResult:
        return self._execute_msgs(msgs, broadcast_func=self.lcd.tx.broadcast_sync, **kwargs)

    def execute_msgs_async(self, msgs: list[Msg], **kwargs) -> AsyncTxBroadcastResult:
        return self._execute_msgs(msgs, broadcast_func=self.lcd.tx.broadcast_async, **kwargs)

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
