import httpx
from terra_sdk.core import Coins

import configs


def get_gas_prices(fcd_host: str = configs.TERRA_FCD_URI) -> Coins:
    res = httpx.get(f'{fcd_host}/v1/txs/gas_prices')
    res.raise_for_status()
    return Coins(res.json())
