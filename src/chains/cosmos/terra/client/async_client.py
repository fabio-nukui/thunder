from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from decimal import Decimal
from typing import AsyncIterable

from terra_sdk.core import AccAddress, Coins
from terra_sdk.core.auth import TxLog
from terra_sdk.core.auth.data import BaseAccount
from terra_sdk.key.mnemonic import MnemonicKey

import auth_secrets
import configs
import utils
from utils.ahttp import AsyncClient
from utils.cache import CacheGroup, ttl_cache

from ...client import BroadcasterMixin, CosmosClient
from ..denoms import UST
from ..token import TerraTokenAmount
from . import utils_rpc
from .api_broadcaster import BroadcasterApi
from .api_market import MarketApi
from .api_mempool import MempoolApi
from .api_oracle import OracleApi
from .api_treasury import TreasuryApi
from .api_tx import TxApi

log = logging.getLogger(__name__)


_CONTRACT_QUERY_CACHE_SIZE = 10_000
_CONTRACT_INFO_CACHE_TTL = 86400  # Contract info should not change; 24h ttl


class TerraClient(CosmosClient, BroadcasterMixin):
    def __init__(
        self,
        lcd_uri: str = configs.TERRA_LCD_URI,
        fcd_uri: str = configs.TERRA_FCD_URI,
        rpc_http_uri: str = configs.TERRA_RPC_HTTP_URI,
        rpc_websocket_uri: str = configs.TERRA_RPC_WEBSOCKET_URI,
        use_broadcaster: bool = configs.TERRA_USE_BROADCASTER,
        broadcaster_uris: list[str] = configs.TERRA_BROADCASTER_URIS,
        broadcast_lcd_uris: list[str] = configs.TERRA_BROADCAST_LCD_URIS,
        chain_id: str = configs.TERRA_CHAIN_ID,
        fee_denom: str = UST.denom,
        gas_prices: Coins.Input = None,
        gas_adjustment: Decimal = configs.TERRA_GAS_ADJUSTMENT,
        raise_on_syncing: bool = configs.RAISE_ON_SYNCING,
        hd_wallet: dict = None,
        hd_wallet_index: int = 0,
    ):
        self.lcd_uri = lcd_uri
        self.fcd_uri = fcd_uri
        self.rpc_http_uri = rpc_http_uri
        self.rpc_websocket_uri = rpc_websocket_uri
        self.use_broadcaster = use_broadcaster
        self.broadcaster_uris = broadcaster_uris
        self.broadcast_lcd_uris = broadcast_lcd_uris
        self.chain_id = chain_id
        self.fee_denom = fee_denom
        self.gas_prices = Coins(gas_prices)
        self.gas_adjustment = gas_adjustment
        self.raise_on_syncing = raise_on_syncing
        self._broadcasters_status: dict[AsyncClient, bool] = {}
        self.active_broadcaster: AsyncClient | None = None

        hd_wallet = auth_secrets.hd_wallet() if hd_wallet is None else hd_wallet
        self.key = MnemonicKey(hd_wallet["mnemonic"], hd_wallet["account"], hd_wallet_index)
        self.height = 0

        self.broadcaster = BroadcasterApi(self)
        self.market = MarketApi(self)
        self.mempool = MempoolApi(self)
        self.oracle = OracleApi(self)
        self.treasury = TreasuryApi(self)
        self.tx = TxApi(self)

    async def start(self):
        self.lcd_http_client = utils.ahttp.AsyncClient(base_url=self.lcd_uri)
        self.fcd_client = utils.ahttp.AsyncClient(base_url=self.fcd_uri)
        self.rpc_http_client = utils.ahttp.AsyncClient(base_url=self.rpc_http_uri)

        await asyncio.gather(self._init_lcd_signer(), self._init_broadcaster_clients())
        await self._check_connections()

        if not self.gas_prices:
            self.lcd.gas_prices = await self.tx.get_gas_prices()
        self.mempool.start()
        await super().start()

    async def _check_connections(self):
        tasks = [
            self.lcd_http_client.check_connection("node_info"),
            self.fcd_client.check_connection("node_info"),
            self.rpc_http_client.check_connection("health"),
        ]
        results = await asyncio.gather(*tasks)
        assert all(results), "Connection error(s)"

        if configs.TERRA_USE_BROADCASTER:
            await self.update_active_broadcaster()
        await asyncio.gather(
            *(conn.check_connection("node_info") for conn in self.broadcast_lcd_clients)
        )

    async def close(self):
        log.debug(f"Closing {self=}")
        self.mempool.stop()
        await asyncio.gather(
            self.lcd_http_client.aclose(),
            self.fcd_client.aclose(),
            self.rpc_http_client.aclose(),
            *(client.aclose() for client in self.broadcast_lcd_clients),
            self.lcd.session.close(),
        )

    @ttl_cache(CacheGroup.TERRA, _CONTRACT_QUERY_CACHE_SIZE)
    async def contract_query(self, contract_addr: AccAddress, query_msg: dict) -> dict:
        return await super().contract_query(contract_addr, query_msg)

    @ttl_cache(CacheGroup.TERRA, _CONTRACT_QUERY_CACHE_SIZE, _CONTRACT_INFO_CACHE_TTL)
    async def contract_info(self, address: AccAddress) -> dict:
        return await super().contract_info(address)

    @ttl_cache(CacheGroup.TERRA)
    async def get_bank(
        self,
        denoms: list[str] = None,
        address: AccAddress = None,
    ) -> list[TerraTokenAmount]:
        address = self.address if address is None else address
        coins_balance, _ = await self.lcd.bank.balance(address)
        return [
            TerraTokenAmount.from_coin(c)
            for c in coins_balance.to_list()
            if denoms is None or c.denom in denoms
        ]

    @ttl_cache(CacheGroup.TERRA)
    async def get_account_data(self, address: AccAddress = None) -> BaseAccount:
        return await super().get_account_data(address)

    async def loop_latest_height(self) -> AsyncIterable[int]:
        async for height in utils_rpc.loop_latest_height(self.rpc_websocket_uri):
            yield height

    @staticmethod
    def extract_log_events(logs: list[TxLog] | None) -> list[dict]:
        if not logs:
            return []
        parsed_logs = []
        for tx_log in logs:
            event_types = [e["type"] for e in tx_log.events]
            assert len(event_types) == len(set(event_types)), "Duplicated event types in events"
            parsed_logs.append({e["type"]: e["attributes"] for e in tx_log.events})
        return parsed_logs

    @staticmethod
    def extract_coin_balance_changes(
        logs: list[TxLog] | None,
    ) -> dict[AccAddress, list[TerraTokenAmount]]:
        if not logs:
            return {}
        changes = defaultdict(list)
        for tx_log in logs:
            if coins_spent := tx_log.events_by_type.get("coin_spent"):
                for addr, str_amount in zip(coins_spent["spender"], coins_spent["amount"]):
                    changes[addr].append(-TerraTokenAmount.from_str(str_amount))
            if coins_received := tx_log.events_by_type.get("coin_received"):
                for addr, str_amount in zip(
                    coins_received["receiver"], coins_received["amount"]
                ):
                    changes[addr].append(TerraTokenAmount.from_str(str_amount))
        return dict(changes)

    @staticmethod
    def parse_from_contract_events(
        events: list[dict],
    ) -> list[dict[str, list[dict[str, str]]]]:
        """Parse contract events in format:
        [  # one object per msg
            {  # one object per contract
                "contract_addr": [  # one object per contract event
                    {  # Example event
                        "action": "transfer",
                        "from": "terra1.....",
                        "to": "terra1....",
                        ...
                    }
                ]
            }
        ]
        """
        logs = []
        for event in events:
            from_contract_logs = event["from_contract"]
            event_logs = defaultdict(list)
            for log_ in from_contract_logs:
                if log_["key"] == "contract_address":
                    contract_logs: dict[str, str] = {}
                    event_logs[log_["value"]].append(contract_logs)
                else:
                    contract_logs[log_["key"]] = log_["value"]
            logs.append(dict(event_logs))
        return logs
