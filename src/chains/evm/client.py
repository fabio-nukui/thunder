import logging

import web3.middleware
from web3 import Account, HTTPProvider, IPCProvider, Web3, WebsocketProvider

import configs

log = logging.getLogger(__name__)

Account.enable_unaudited_hdwallet_features()


ETHEREUM_CHAIN_ID = 1
ETH_COIN_TYPE = 60

BSC_CHAIN_ID = 56
BNB_COIN_TYPE = 714

DEFAULT_CONN_TIMEOUT = 3


class EVMClient:
    def __init__(
        self,
        hd_wallet: dict,
        endpoint_uri: str,
        chain_id: int,
        coin_type: int,
        middlewares: list[str] = None,
        hd_wallet_index: int = 0,
        timeout: int = DEFAULT_CONN_TIMEOUT,
    ):
        self.endpoint_uri = endpoint_uri
        self.chain_id = chain_id
        self.middlewares = middlewares
        self.timeout = timeout

        self.w3 = get_w3(endpoint_uri, middlewares, timeout)

        self.account = Account.from_mnemonic(
            hd_wallet['mnemonic'],
            account_path=f"m/44'/{coin_type}'/{hd_wallet['account']}'/0/{hd_wallet_index}",
        )

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}(endpoint_uri={self.endpoint_uri},'
            f'address={self.account.address})'
        )


class EthereumClient(EVMClient):
    def __init__(
        self,
        hd_wallet: dict,
        endpoint_uri: str = configs.ETHEREUM_RPC_URI,
        hd_wallet_index: int = 0,
        timeout: int = DEFAULT_CONN_TIMEOUT,
    ):
        super().__init__(
            hd_wallet=hd_wallet,
            endpoint_uri=endpoint_uri,
            chain_id=ETHEREUM_CHAIN_ID,
            coin_type=ETH_COIN_TYPE,
            middlewares=configs.ETHEREUM_WEB3_MIDDEWARES,
            hd_wallet_index=hd_wallet_index,
            timeout=timeout,
        )


class BSCClient(EVMClient):
    def __init__(
        self,
        hd_wallet: dict,
        endpoint_uri: str = configs.BSC_RPC_URI,
        hd_wallet_index: int = 0,
        timeout: int = DEFAULT_CONN_TIMEOUT,
    ):
        super().__init__(
            hd_wallet=hd_wallet,
            endpoint_uri=endpoint_uri,
            chain_id=BSC_CHAIN_ID,
            coin_type=BNB_COIN_TYPE,
            middlewares=configs.BSC_WEB3_MIDDEWARES,
            hd_wallet_index=hd_wallet_index,
            timeout=timeout,
        )


def get_w3(
    endpoint_uri: str,
    middlewares: list[str] = None,
    timeout: int = DEFAULT_CONN_TIMEOUT,
) -> Web3:
    if endpoint_uri.startswith('http'):
        provider = HTTPProvider(endpoint_uri, request_kwargs={'timeout': timeout})
    elif endpoint_uri.startswith('wss'):
        provider = WebsocketProvider(endpoint_uri, websocket_timeout=timeout)
    elif endpoint_uri.endswith('ipc'):
        provider = IPCProvider(endpoint_uri, timeout)
    else:
        raise ValueError(f'Invalid {endpoint_uri=}')

    if middlewares is None:
        middlewares = []
    else:
        middlewares = [getattr(web3.middleware, m) for m in middlewares if m]

    return Web3(provider, middlewares)
