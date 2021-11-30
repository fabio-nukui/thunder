from __future__ import annotations

import logging
from collections import defaultdict
from decimal import Decimal

import cosmos_proto.cosmos.bank.v1beta1 as cosmos_bank_pb
from cosmos_sdk.core import AccAddress, Coins
from cosmos_sdk.core.auth.data import BaseAccount
from cosmos_sdk.core.tx import TxLog

import auth_secrets
import configs
from utils.cache import CacheGroup, ttl_cache

from ...client import CosmosClient
from ..denoms import OSMO
from ..mnemonic_key import MnemonicKey
from ..token import OsmosisTokenAmount
from .api_tx import TxApi
from .gamm_api import GammApi

log = logging.getLogger(__name__)

_CONTRACT_QUERY_CACHE_SIZE = 10_000
_CONTRACT_INFO_CACHE_TTL = 86400  # Contract info should not change; 24h ttl


class OsmosisClient(CosmosClient):
    tx: TxApi  # Override superclass' property

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
        allow_concurrent_pool_arbs: bool = False,
        fee_denom: str = OSMO.denom,
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
            allow_concurrent_pool_arbs=allow_concurrent_pool_arbs,
            fee_denom=fee_denom,
            gas_prices=gas_prices,
            gas_adjustment=gas_adjustment,
            raise_on_syncing=raise_on_syncing,
        )

        self.gamm = GammApi(self)
        self.tx = TxApi(self)

        hd_wallet = auth_secrets.hd_wallet() if hd_wallet is None else hd_wallet
        self.key = MnemonicKey(hd_wallet["mnemonic"], hd_wallet["account"], hd_wallet_index)

    async def start(self):
        await super().start()

        if not self.gas_prices:
            self.gas_prices = self.lcd.gas_prices = Coins(f"0{OSMO.denom}")

        self.grpc_bank = cosmos_bank_pb.QueryStub(self.grpc_channel)
        self.gamm.start()

        await self.update_active_broadcaster()

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
        return OsmosisTokenAmount.from_coin(res.balance, self.chain_id)

    @ttl_cache(CacheGroup.OSMOSIS)
    async def get_all_balances(self, address: AccAddress = None) -> list[OsmosisTokenAmount]:
        address = self.address if address is None else address
        res = await self.grpc_bank.all_balances(address=address)
        if res.pagination.next_key:
            raise NotImplementedError("not implemented for paginated results")
        return [OsmosisTokenAmount.from_coin(c, self.chain_id) for c in res.balances]

    @ttl_cache(CacheGroup.OSMOSIS)
    async def get_account_data(self, address: AccAddress = None) -> BaseAccount:
        return await super().get_account_data(address)

    @staticmethod
    def get_coin_balance_changes(
        logs: list[TxLog] | None,
    ) -> dict[AccAddress, list[OsmosisTokenAmount]]:
        if not logs:
            return {}
        changes: dict[AccAddress, list[OsmosisTokenAmount]] = defaultdict(list)
        for tx_log in logs:
            if not (transfers := tx_log.events_by_type.get("transfer")):
                continue
            senders = [AccAddress(addr) for addr in transfers["sender"]]
            recipients = [AccAddress(addr) for addr in transfers["recipient"]]
            amounts = [OsmosisTokenAmount.from_str(a) for a in transfers["amount"]]
            for sender, recipient, amount in zip(senders, recipients, amounts):
                changes[sender].append(-amount)
                changes[recipient].append(amount)
        return dict(changes)
