from decimal import Decimal

import auth_secrets
import configs
from common.blockchain_client import AsyncBlockchainClient

from ..mnemonic_key import MnemonicKey


class OsmosisClient(AsyncBlockchainClient):
    def __init__(
        self,
        lcd_uri: str = configs.OSMOSIS_LCD_URI,
        rpc_http_uri: str = configs.OSMOSIS_RPC_HTTP_URI,
        rpc_websocket_uri: str = configs.OSMOSIS_RPC_WEBSOCKET_URI,
        use_broadcaster: bool = configs.OSMOSIS_USE_BROADCASTER,
        broadcaster_uris: list[str] = configs.OSMOSIS_BROADCASTER_URIS,
        broadcast_lcd_uris: list[str] = configs.OSMOSIS_BROADCAST_LCD_URIS,
        chain_id: str = configs.OSMOSIS_CHAIN_ID,
        gas_prices: str = None,
        gas_adjustment: Decimal = configs.OSMOSIS_GAS_ADJUSTMENT,
        raise_on_syncing: bool = configs.RAISE_ON_SYNCING,
        hd_wallet: dict = None,
        hd_wallet_index: int = 0,
    ):
        self.lcd_uri = lcd_uri
        self.rpc_http_uri = rpc_http_uri
        self.rpc_websocket_uri = rpc_websocket_uri
        self.use_broadcaster = use_broadcaster
        self.broadcaster_uris = broadcaster_uris
        self.broadcast_lcd_uris = broadcast_lcd_uris
        self.chain_id = chain_id
        self.gas_prices = gas_prices
        self.gas_adjustment = gas_adjustment

        hd_wallet = auth_secrets.hd_wallet() if hd_wallet is None else hd_wallet
        self.key = MnemonicKey(hd_wallet["mnemonic"], hd_wallet["account"], hd_wallet_index)
        self.height = 0

        super().__init__(raise_on_syncing)
