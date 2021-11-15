from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
from collections import defaultdict
from decimal import Decimal
from typing import AsyncIterable

from terra_sdk.client.lcd.api.tx import SignerOptions
from terra_sdk.core import AccAddress, Coins
from terra_sdk.core.auth import TxLog
from terra_sdk.core.auth.data import BaseAccount
from terra_sdk.exceptions import LCDResponseError
from terra_sdk.key.mnemonic import MnemonicKey

import auth_secrets
import configs
import utils
from common.blockchain_client import AsyncBlockchainClient
from exceptions import NotContract
from utils.ahttp import AsyncClient
from utils.cache import CacheGroup, ttl_cache

from ..denoms import UST
from ..token import TerraTokenAmount
from . import utils_rpc
from .api_broadcaster import BroadcasterApi
from .api_market import MarketApi
from .api_mempool import MempoolApi
from .api_oracle import OracleApi
from .api_treasury import TreasuryApi
from .api_tx import TxApi
from .lcd import AsyncLCDClient2

log = logging.getLogger(__name__)


TERRA_CONTRACT_QUERY_CACHE_SIZE = 10_000
CONTRACT_INFO_CACHE_TTL = 86400  # Contract info should not change; 24h ttl
MAX_BROADCASTER_HEIGHT_DIFFERENCE = 2
_pat_missing_contract = re.compile(r"contract terra1(\w+): not found")


class TerraClient(AsyncBlockchainClient):
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
        self.gas_adjustment = Decimal(gas_adjustment)
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
        await self._fix_broadcaster_urls()
        self.lcd_http_client = utils.ahttp.AsyncClient(base_url=self.lcd_uri)
        self.fcd_client = utils.ahttp.AsyncClient(base_url=self.fcd_uri)
        self.rpc_http_client = utils.ahttp.AsyncClient(base_url=self.rpc_http_uri)
        self._broadcaster_clients = [
            utils.ahttp.AsyncClient(base_url=url, verify=False) for url in self.broadcaster_uris
        ]
        self._broadcasters_status = {c: False for c in self._broadcaster_clients}
        self.broadcast_lcd_clients = [
            utils.ahttp.AsyncClient(base_url=url) for url in self.broadcast_lcd_uris
        ]

        self.lcd = AsyncLCDClient2(
            self.lcd_uri, self.chain_id, self.gas_prices, self.gas_adjustment
        )
        self.wallet = self.lcd.wallet(self.key)
        self.address = self.wallet.key.acc_address
        self.signer = SignerOptions(self.address, public_key=self.wallet.key.public_key)

        try:
            self.height = await self.get_latest_height()
            self.signer.sequence = (await self.get_account_data()).sequence
        except Exception:
            await self.lcd.session.close()
            raise
        await self._check_connections()

        if not self.gas_prices:
            self.lcd.gas_prices = await self.tx.get_gas_prices()
        self.mempool.start()
        await super().start()

    async def _fix_broadcaster_urls(self):
        host_ip = await utils.ahttp.get_host_ip()
        self.broadcaster_uris = [
            url.replace(host_ip, "localhost") for url in self.broadcaster_uris
        ]
        self.broadcast_lcd_uris = [url for url in self.broadcast_lcd_uris if host_ip not in url]

    async def _check_connections(self):
        tasks = [
            self.lcd_http_client.check_connection("node_info"),
            self.fcd_client.check_connection("node_info"),
            self.rpc_http_client.check_connection("health"),
        ]
        results = await asyncio.gather(*tasks)
        assert all(results), "Connection error(s)"
        await self.update_active_broadcaster()
        if not self.use_broadcaster:
            await asyncio.gather(
                *(conn.check_connection("node_info") for conn in self.broadcast_lcd_clients)
            )

    async def update_active_broadcaster(self):
        if not configs.TERRA_USE_BROADCASTER:
            return
        tasks = (self._set_broadcaster_status(c) for c in self._broadcaster_clients)
        await asyncio.gather(*tasks)

        n_ok = sum(self._broadcasters_status.values())
        n_total = len(self._broadcasters_status)
        log.debug(f"{n_ok}/{n_total} broadcasters OK")

        if self.use_broadcaster and not n_ok:
            log.info("Stop using broadcaster")
            self.use_broadcaster = False
        elif not self.use_broadcaster and n_ok:
            log.info("Start using broadcaster")
            self.use_broadcaster = True

        if self.use_broadcaster:
            for client, status_ok in self._broadcasters_status.items():
                if status_ok:
                    if self.active_broadcaster != client:
                        log.info(f"Switching broadcaster to {client.base_url}")
                        self.active_broadcaster = client
                    return

    async def _set_broadcaster_status(self, broadcaster_client: AsyncClient):
        try:
            res = await broadcaster_client.get("lcd/blocks/latest", supress_logs=True)
            height = int(res.json()["block"]["header"]["height"])
            if self.height - height > MAX_BROADCASTER_HEIGHT_DIFFERENCE:
                raise Exception(f"Broadcaster {height=} behind {self.height=}")
        except Exception as e:
            previous_status = self._broadcasters_status.get(broadcaster_client)
            if previous_status or previous_status is None:
                log.debug(f"Error with broadcaster={broadcaster_client.base_url}: {e!r}")
                self._broadcasters_status[broadcaster_client] = False
        else:
            self._broadcasters_status[broadcaster_client] = True

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(lcd.url={self.lcd.url}, chain_id={self.chain_id}, "
            f"account={self.key.acc_address})"
        )

    async def is_syncing(self) -> bool:
        return await self.lcd.tendermint.syncing()

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

    @ttl_cache(CacheGroup.TERRA, TERRA_CONTRACT_QUERY_CACHE_SIZE)
    async def contract_query(self, contract_addr: AccAddress, query_msg: dict) -> dict:
        try:
            return await self.lcd.wasm.contract_query(contract_addr, query_msg)
        except LCDResponseError as e:
            if e.response.status == 500 and (match := _pat_missing_contract.search(e.message)):
                raise NotContract(match.group(1))
            else:
                raise e

    @ttl_cache(CacheGroup.TERRA, TERRA_CONTRACT_QUERY_CACHE_SIZE, CONTRACT_INFO_CACHE_TTL)
    async def contract_info(self, address: AccAddress) -> dict:
        try:
            return await self.lcd.wasm.contract_info(address)
        except LCDResponseError as e:
            if e.response.status == 500:
                raise NotContract
            raise e

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
        address = self.address if address is None else address
        return await self.lcd.auth.account_info(address)

    async def get_account_number(self, address: AccAddress = None) -> int:
        return (await self.get_account_data(address)).account_number

    async def get_account_sequence(self, address: AccAddress = None) -> int:
        on_chain = (await self.get_account_data(address)).sequence
        local = self.signer.sequence or 0
        if on_chain == local:
            return on_chain
        if on_chain > local:
            log.debug(f"Using higher on-chain sequence value ({on_chain=}, {local=})")
            self.signer.sequence = on_chain
            return on_chain
        log.debug(f"Using higher local sequence value ({local=}, {on_chain=})")
        return self.signer.sequence or 0

    @staticmethod
    def encode_msg(msg: dict) -> str:
        bytes_json = json.dumps(msg, separators=(",", ":")).encode("utf-8")
        return base64.b64encode(bytes_json).decode("ascii")

    async def get_latest_height(self) -> int:
        info = await self.lcd.tendermint.block_info()
        return int(info["block"]["header"]["height"])

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
