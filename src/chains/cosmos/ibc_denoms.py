import json
from functools import lru_cache

_COMMON_IBC_DENOMS_FILE = "resources/contracts/cosmos/{chain_id}/ibc_main_denoms.json"


@lru_cache()
def get_ibc_denom(name: str, chain_id: str) -> str:
    with open(_COMMON_IBC_DENOMS_FILE.format(chain_id=chain_id)) as f:
        return json.load(f)[name]
