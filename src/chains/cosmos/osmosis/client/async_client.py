import asyncio
import logging
from decimal import Decimal

import grpclib.client
import osmosis_proto.cosmos.bank.v1beta1 as cosmos_bank_pb
import osmosis_proto.osmosis.gamm.v1beta1 as osmosis_gamm_pb
from terra_sdk.core import AccAddress, Coins
from terra_sdk.core.auth.data import BaseAccount

import auth_secrets
import configs
import utils
from utils.cache import CacheGroup, ttl_cache

from ...client import BroadcasterMixin, CosmosClient
from ..mnemonic_key import MnemonicKey
from ..token import OsmosisTokenAmount
from .gamm_api import GammApi

log = logging.getLogger(__name__)

_CONTRACT_QUERY_CACHE_SIZE = 10_000
_CONTRACT_INFO_CACHE_TTL = 86400  # Contract info should not change; 24h ttl
_NATIVE_OSMO_DENOM = "uosmo"


class OsmosisClient(BroadcasterMixin, CosmosClient):
    def __init__(
        self,
        lcd_uri: str = configs.OSMOSIS_LCD_URI,
        rpc_http_uri: str = configs.OSMOSIS_RPC_HTTP_URI,
        rpc_websocket_uri: str = configs.OSMOSIS_RPC_WEBSOCKET_URI,
        grpc_uri: str = configs.OSMOSIS_GRPC_URI,
        use_broadcaster: bool = configs.OSMOSIS_USE_BROADCASTER,
        broadcaster_uris: list[str] = configs.OSMOSIS_BROADCASTER_URIS,
        broadcast_lcd_uris: list[str] = configs.OSMOSIS_BROADCAST_LCD_URIS,
        chain_id: str = configs.OSMOSIS_CHAIN_ID,
        fee_denom: str = _NATIVE_OSMO_DENOM,
        gas_prices: Coins.Input = None,
        gas_adjustment: Decimal = configs.OSMOSIS_GAS_ADJUSTMENT,
        raise_on_syncing: bool = configs.RAISE_ON_SYNCING,
        hd_wallet: dict = None,
        hd_wallet_index: int = 0,
    ):
        super().__init__(
            lcd_uri=lcd_uri,
            rpc_http_uri=rpc_http_uri,
            rpc_websocket_uri=rpc_websocket_uri,
            grpc_uri=grpc_uri,
            use_broadcaster=use_broadcaster,
            broadcaster_uris=broadcaster_uris,
            broadcast_lcd_uris=broadcast_lcd_uris,
            chain_id=chain_id,
            fee_denom=fee_denom,
            gas_prices=gas_prices,
            gas_adjustment=gas_adjustment,
            raise_on_syncing=raise_on_syncing,
        )

        self.gamm = GammApi(self)

        hd_wallet = auth_secrets.hd_wallet() if hd_wallet is None else hd_wallet
        self.key = MnemonicKey(hd_wallet["mnemonic"], hd_wallet["account"], hd_wallet_index)

    async def start(self):
        self.lcd_http_client = utils.ahttp.AsyncClient(base_url=self.lcd_uri)
        self.rpc_http_client = utils.ahttp.AsyncClient(base_url=self.rpc_http_uri)

        grpc_url, grpc_port = self.grpc_uri.split(":")
        self.grpc_channel = grpclib.client.Channel(grpc_url, int(grpc_port))

        self.grpc_gamm = osmosis_gamm_pb.QueryStub(self.grpc_channel)
        self.grpc_bank = cosmos_bank_pb.QueryStub(self.grpc_channel)

        await asyncio.gather(self._init_lcd_signer(), self._init_broadcaster_clients())
        await self._check_connections()

        if not self.gas_prices:
            self.lcd.gas_prices = Coins("0uosmo")
        await super().start()

    async def _check_connections(self):
        tasks = [
            self.lcd_http_client.check_connection("node_info"),
            self.rpc_http_client.check_connection("health"),
        ]
        results = await asyncio.gather(*tasks)
        assert all(results), "Connection error(s)"

        if configs.OSMOSIS_USE_BROADCASTER:
            await self.update_active_broadcaster()
        await asyncio.gather(
            *(conn.check_connection("node_info") for conn in self.broadcast_lcd_clients)
        )

    async def close(self):
        log.debug(f"Closing {self=}")
        await asyncio.gather(
            self.lcd_http_client.aclose(),
            self.rpc_http_client.aclose(),
            *(client.aclose() for client in self.broadcast_lcd_clients),
            self.lcd.session.close(),
        )

    @ttl_cache(CacheGroup.OSMOSIS, _CONTRACT_QUERY_CACHE_SIZE)
    async def contract_query(self, contract_addr: AccAddress, query_msg: dict) -> dict:
        return await super().contract_query(contract_addr, query_msg)

    @ttl_cache(CacheGroup.OSMOSIS, _CONTRACT_QUERY_CACHE_SIZE, _CONTRACT_INFO_CACHE_TTL)
    async def contract_info(self, address: AccAddress) -> dict:
        return await super().contract_info(address)

    @ttl_cache(CacheGroup.OSMOSIS)
    async def get_balance(self, denom: str, address: AccAddress = None) -> OsmosisTokenAmount:
        address = self.address if address is None else address
        res = await self.grpc_bank.balance(address=address, denom=denom)
        return OsmosisTokenAmount.from_coin(res.balance, self)

    @ttl_cache(CacheGroup.OSMOSIS)
    async def get_all_balances(self, address: AccAddress = None) -> list[OsmosisTokenAmount]:
        address = self.address if address is None else address
        res = await self.grpc_bank.all_balances(address=address)
        if res.pagination.next_key:
            raise NotImplementedError("not implemented for paginated results")
        return [OsmosisTokenAmount.from_coin(c, self) for c in res.balances]

    @ttl_cache(CacheGroup.OSMOSIS)
    async def get_account_data(self, address: AccAddress = None) -> BaseAccount:
        return await super().get_account_data(address)
