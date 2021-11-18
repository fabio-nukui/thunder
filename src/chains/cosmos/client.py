from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
from abc import ABC, abstractmethod
from decimal import Decimal

from terra_sdk.client.lcd.api.tx import SignerOptions
from terra_sdk.core import AccAddress, Coins
from terra_sdk.core.auth.data import BaseAccount
from terra_sdk.core.tx import TxLog
from terra_sdk.exceptions import LCDResponseError
from terra_sdk.key.mnemonic import MnemonicKey

import utils
from common.blockchain_client import AsyncBlockchainClient
from exceptions import NotContract
from utils.ahttp import AsyncClient

from .lcd import AsyncLCDClient

log = logging.getLogger(__name__)

MAX_BROADCASTER_HEIGHT_DIFFERENCE = 2
_PAT_MISSING_CONTRACT = re.compile(r"contract (\w+): not found")


class CosmosClient(AsyncBlockchainClient, ABC):
    key: MnemonicKey

    def __init__(
        self,
        lcd_uri: str,
        rpc_http_uri: str,
        rpc_websocket_uri: str,
        fee_denom: str,
        gas_prices: Coins.Input | None,
        gas_adjustment: Decimal,
        chain_id: str,
        *args,
        **kwargs,
    ):
        self.lcd_uri = lcd_uri
        self.rpc_http_uri = rpc_http_uri
        self.rpc_websocket_uri = rpc_websocket_uri

        self.fee_denom = fee_denom
        self.gas_prices = Coins(gas_prices)
        self.gas_adjustment = gas_adjustment
        self.chain_id = chain_id
        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(lcd.url={self.lcd.url}, chain_id={self.chain_id}, "
            f"account={self.address})"
        )

    async def _init_lcd_signer(self):
        self.lcd = AsyncLCDClient(
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

    @abstractmethod
    async def get_bank(
        self,
        denoms: list[str] = None,
        address: AccAddress = None,
    ) -> list:
        ...

    async def is_syncing(self) -> bool:
        return await self.lcd.tendermint.syncing()

    async def get_latest_height(self) -> int:
        info = await self.lcd.tendermint.block_info()
        return int(info["block"]["header"]["height"])

    async def contract_query(self, contract_addr: AccAddress, query_msg: dict) -> dict:
        try:
            return await self.lcd.wasm.contract_query(contract_addr, query_msg)
        except LCDResponseError as e:
            if e.response.status == 500 and (match := _PAT_MISSING_CONTRACT.search(e.message)):
                raise NotContract(match.group(1))
            else:
                raise e

    async def contract_info(self, address: AccAddress) -> dict:
        try:
            return await self.lcd.wasm.contract_info(address)
        except LCDResponseError as e:
            if e.response.status == 500:
                raise NotContract
            raise e

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


class BroadcasterMixin:
    height: int

    def __init__(
        self,
        use_broadcaster: bool,
        broadcaster_uris: list[str],
        broadcast_lcd_uris: list[str],
        *args,
        **kwargs,
    ):
        self.use_broadcaster = use_broadcaster
        self.broadcaster_uris = broadcaster_uris
        self.broadcast_lcd_uris = broadcast_lcd_uris

        self._broadcaster_clients: list[AsyncClient] = []
        self._broadcasters_status: dict[AsyncClient, bool] = {}
        self.broadcast_lcd_clients: list[AsyncClient] = []
        self.active_broadcaster: AsyncClient | None = None

        super().__init__(*args, **kwargs)  # type: ignore

    async def _init_broadcaster_clients(self):
        await self._fix_broadcaster_urls()
        self._broadcaster_clients = [
            utils.ahttp.AsyncClient(base_url=url, verify=False) for url in self.broadcaster_uris
        ]
        self._broadcasters_status = {c: False for c in self._broadcaster_clients}
        self.broadcast_lcd_clients = [
            utils.ahttp.AsyncClient(base_url=url) for url in self.broadcast_lcd_uris
        ]

    async def _fix_broadcaster_urls(self):
        host_ip = await utils.ahttp.get_host_ip()
        self.broadcaster_uris = [
            url.replace(host_ip, "localhost") for url in self.broadcaster_uris
        ]
        self.broadcast_lcd_uris = [url for url in self.broadcast_lcd_uris if host_ip not in url]

    async def update_active_broadcaster(self):
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
