import configs

from . import curve, lido, one_inch
from .client import EVMClient
from .core import ERC20Token, EVMNativeToken, EVMToken, EVMTokenAmount
from .one_inch import OneInchExchange

__all__ = [
    "curve",
    "lido",
    "one_inch",
    "EthereumClient",
    "BSCClient",
    "ERC20Token",
    "EVMNativeToken",
    "EVMToken",
    "EVMTokenAmount",
    "OneInchExchange",
]


# BIP-44 coin types (https://github.com/satoshilabs/slips/blob/master/slip-0044.md)
_ETH_COIN_TYPE = 60
_BNB_COIN_TYPE = 714


class EthereumClient(EVMClient):
    def __init__(
        self,
        hd_wallet: dict = None,
        endpoint_uri: str = configs.ETHEREUM_RPC_URI,
        hd_wallet_index: int = 0,
        timeout: int = None,
    ):
        super().__init__(
            endpoint_uri=endpoint_uri,
            chain_id=configs.ETHEREUM_CHAIN_ID,
            coin_type=_ETH_COIN_TYPE,
            middlewares=configs.ETHEREUM_WEB3_MIDDEWARES,
            hd_wallet=hd_wallet,
            hd_wallet_index=hd_wallet_index,
            timeout=timeout,
        )


class BSCClient(EVMClient):
    def __init__(
        self,
        hd_wallet: dict = None,
        endpoint_uri: str = configs.BSC_RPC_URI,
        hd_wallet_index: int = 0,
        timeout: int = None,
    ):
        super().__init__(
            endpoint_uri=endpoint_uri,
            chain_id=configs.BSC_CHAIN_ID,
            coin_type=_BNB_COIN_TYPE,
            middlewares=configs.BSC_WEB3_MIDDEWARES,
            hd_wallet=hd_wallet,
            hd_wallet_index=hd_wallet_index,
            timeout=timeout,
        )
