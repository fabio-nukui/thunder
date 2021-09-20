import httpx
from terra_sdk.client.lcd import LCDClient, Wallet
from terra_sdk.core import Coins
from terra_sdk.key.mnemonic import MnemonicKey

import configs
import utils


def get_wallet() -> Wallet:
    secret = utils.aws.get_secret(configs.MNEMONIC_SECRET_NAME, decode_json=True)
    key = MnemonicKey(secret['mnemonic'], int(secret.get('account', 0)))
    lcd = LCDClient(
        url=configs.LCD_URI,
        chain_id=configs.CHAIN_ID,
        gas_prices=get_gas_prices(),
        gas_adjustment=configs.TERRA_GAS_ADJUSTMENT,
    )
    return lcd.wallet(key)


def get_gas_prices() -> Coins:
    res = httpx.get(f'{configs.FCD_URI}/v1/txs/gas_prices')
    res.raise_for_status()
    return Coins(res.json())
