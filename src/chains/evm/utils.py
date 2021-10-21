import json
from functools import cache

_ADDRESSES_FILE_PATH = "resources/addresses/evm/{chain_id}/cw20_whitelist.json"


@cache
def get_erc20_addresses(chain_id: int) -> dict[str, str]:
    return json.load(open(_ADDRESSES_FILE_PATH.format(chain_id)))
