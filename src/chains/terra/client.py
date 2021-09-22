import httpx
from terra_sdk.client.lcd import LCDClient
from terra_sdk.core import Coins
from terra_sdk.key.mnemonic import MnemonicKey

import auth_secrets
import configs
from utils.cache import CacheGroup, ttl_cache

TERRA_CONTRACT_QUERY_CACHE_SIZE = 1000
TERRA_GAS_PRICE_CACHE_TTL = 3600


class TerraClient:
    def __init__(
        self,
        lcd_uri: str = configs.TERRA_LCD_URI,
        fcd_uri: str = configs.TERRA_FCD_URI,
        chain_id: str = configs.TERRA_CHAIN_ID,
        gas_prices: Coins.Input = None,
        gas_adjustment: float = configs.TERRA_GAS_ADJUSTMENT,
        hd_wallet_index: int = 0,
    ):
        self.lcd_uri = lcd_uri
        self.fcd_uri = fcd_uri
        self.chain_id = chain_id

        gas_prices = self.get_gas_prices() if gas_prices is None else gas_prices
        lcd = LCDClient(lcd_uri, chain_id, gas_prices, gas_adjustment)

        secret = auth_secrets.hd_wallet()
        key = MnemonicKey(secret['mnemonic'], secret['account'], hd_wallet_index)
        wallet = lcd.wallet(key)

        self.lcd = lcd
        self.key = key
        self.wallet = wallet

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}(lcd_uri={self.lcd_uri},(chain_id={self.chain_id}),'
            f'account={self.key.acc_address})'
        )

    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=configs.TERRA_CACHE_TTL)
    def get_gas_prices(self) -> Coins:
        res = httpx.get(f'{self.fcd_uri}/v1/txs/gas_prices')
        res.raise_for_status()
        return Coins(res.json())

    @ttl_cache(CacheGroup.TERRA, TERRA_CONTRACT_QUERY_CACHE_SIZE)
    def contract_query(self, contract_addr: str, query_msg: dict) -> dict:
        return self.lcd.wasm.contract_query(contract_addr, query_msg)
