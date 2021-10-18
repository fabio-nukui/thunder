from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
from collections import defaultdict
from decimal import Decimal
from typing import AsyncIterable

from terra_sdk.client.lcd import AsyncLCDClient
from terra_sdk.core import AccAddress, Coins
from terra_sdk.core.auth import TxLog
from terra_sdk.exceptions import LCDResponseError
from terra_sdk.key.mnemonic import MnemonicKey

import auth_secrets
import configs
import utils
from exceptions import NotContract
from utils.cache import CacheGroup, ttl_cache

from ..denoms import UST
from ..interfaces import ITerraClient
from ..token import TerraTokenAmount
from . import utils_rpc
from .api_market import MarketApi
from .api_mempool import MempoolApi
from .api_oracle import OracleApi
from .api_treasury import TreasuryApi
from .api_tx import TxApi

log = logging.getLogger(__name__)


TERRA_CONTRACT_QUERY_CACHE_SIZE = 10_000
CONTRACT_INFO_CACHE_TTL = 86400  # Contract info should not change; 24h ttl
_pat_contract_not_found = re.compile(r"contract terra1(\w+): not found")


class TerraClient(ITerraClient):
    market: MarketApi
    oracle: OracleApi
    mempool: MempoolApi
    treasury: TreasuryApi
    tx: TxApi

    @classmethod
    async def new(
        cls,
        hd_wallet: dict = None,
        lcd_uri: str = configs.TERRA_LCD_URI,
        fcd_uri: str = configs.TERRA_FCD_URI,
        rpc_http_uri: str = configs.TERRA_RPC_HTTP_URI,
        rpc_websocket_uri: str = configs.TERRA_RPC_WEBSOCKET_URI,
        broadcast_lcd_uris: list[str] = configs.TERRA_BROADCAST_LCD_URIS,
        chain_id: str = configs.TERRA_CHAIN_ID,
        fee_denom: str = UST.denom,
        gas_prices: Coins.Input = None,
        gas_adjustment: Decimal = configs.TERRA_GAS_ADJUSTMENT,
        hd_wallet_index: int = 0,
        raise_on_syncing: bool = configs.RAISE_ON_SYNCING,
    ) -> TerraClient:
        self = super().__new__(cls)

        self.lcd_http_client = utils.ahttp.AsyncClient(base_url=lcd_uri)
        self.fcd_client = utils.ahttp.AsyncClient(base_url=fcd_uri)
        self.rpc_http_client = utils.ahttp.AsyncClient(base_url=rpc_http_uri)
        self.rpc_websocket_uri = rpc_websocket_uri
        self.broadcast_lcd_clients = [
            utils.ahttp.AsyncClient(base_url=url) for url in broadcast_lcd_uris if url != ""
        ]
        self.chain_id = chain_id
        self.fee_denom = fee_denom
        self.gas_adjustment = Decimal(gas_adjustment)

        hd_wallet = auth_secrets.hd_wallet() if hd_wallet is None else hd_wallet
        key = MnemonicKey(hd_wallet["mnemonic"], hd_wallet["account"], hd_wallet_index)
        self.key = key  # Set key before get_gas_prices() to avoid error with cache debugging

        self.lcd = AsyncLCDClient(lcd_uri, chain_id, gas_prices, self.gas_adjustment)
        self.wallet = self.lcd.wallet(key)
        self.address = self.wallet.key.acc_address
        self.height = await self.get_latest_height()
        self.account_sequence = 0

        self.market = MarketApi(self)
        self.mempool = MempoolApi(self)
        self.oracle = OracleApi(self)
        self.treasury = TreasuryApi(self)
        self.tx = TxApi(self)

        if gas_prices is None:
            self.lcd.gas_prices = await self.tx.get_gas_prices()

        await self.init(raise_on_syncing)
        return self

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(lcd.url={self.lcd.url}, chain_id={self.chain_id}, "
            f"account={self.key.acc_address})"
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return await self.close()

    async def is_syncing(self) -> bool:
        return await self.lcd.tendermint.syncing()

    async def close(self):
        await asyncio.gather(
            self.lcd.session.close(),
            self.lcd_http_client.aclose(),
            self.fcd_client.aclose(),
            self.rpc_http_client.aclose(),
            self.mempool.close(),
        )

    @ttl_cache(CacheGroup.TERRA, TERRA_CONTRACT_QUERY_CACHE_SIZE)
    async def contract_query(self, contract_addr: AccAddress, query_msg: dict) -> dict:
        try:
            return await self.lcd.wasm.contract_query(contract_addr, query_msg)
        except LCDResponseError as e:
            if e.response.status == 500 and (match := _pat_contract_not_found.search(e.message)):
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
        self, denoms: list[str] = None, address: str = None
    ) -> list[TerraTokenAmount]:
        address = self.address if address is None else address
        coins_balance = await self.lcd.bank.balance(address)
        return [
            TerraTokenAmount.from_coin(c)
            for c in coins_balance
            if denoms is None or c.denom in denoms
        ]

    @ttl_cache(CacheGroup.TERRA)
    async def get_account_number(self, address: AccAddress = None) -> int:
        address = self.address if address is None else address
        return (await self.lcd.auth.account_info(address)).account_number

    async def get_account_sequence(self, address: AccAddress = None) -> int:
        address = self.address if address is None else address
        on_chain = (await self.lcd.auth.account_info(address)).sequence
        local = self.account_sequence
        if on_chain == local:
            return on_chain
        if on_chain > local:
            log.debug(f"Using higher on-chain sequence value ({on_chain=}, {local=})")
            self.account_sequence = on_chain
            return on_chain
        log.debug(f"Using higher local sequence value ({local=}, {on_chain=})")
        return self.account_sequence

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
    def extract_log_events(logs: list[TxLog]) -> list[dict]:
        parsed_logs = []
        for tx_log in logs:
            event_types = [e["type"] for e in tx_log.events]
            assert len(event_types) == len(set(event_types)), "Duplicated event types in events"
            parsed_logs.append({e["type"]: e["attributes"] for e in tx_log.events})
        return parsed_logs

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
                        "from": "terra.....",
                        "to": "terra....",
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
