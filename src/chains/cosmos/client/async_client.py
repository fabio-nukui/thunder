from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
from abc import ABC, abstractmethod
from collections import defaultdict
from decimal import Decimal
from typing import AsyncIterable

import cosmos_proto.cosmos.auth.v1beta1 as cosmos_auth_pb
import cosmos_proto.cosmos.base.tendermint.v1beta1 as cosmos_tendermint_tb
import grpclib.client
from cosmos_sdk.client.lcd.api.tx import SignerOptions
from cosmos_sdk.core import AccAddress, Coins
from cosmos_sdk.core.auth.data import BaseAccount
from cosmos_sdk.core.tx import TxLog
from cosmos_sdk.exceptions import LCDResponseError
from cosmos_sdk.key.mnemonic import MnemonicKey

import utils
from chains.cosmos.token import CosmosTokenAmount
from common.blockchain_client import AsyncBlockchainClient
from exceptions import NotContract

from .. import utils_rpc
from ..lcd import AsyncLCDClient
from .api_broadcaster import BroadcasterApi
from .api_ibc import IbcApi
from .api_mempool import MempoolApi
from .api_tx import TxApi
from .broadcaster_mixin import BroadcasterMixin

log = logging.getLogger(__name__)

_PAT_MISSING_CONTRACT = re.compile(r"contract (\w+): not found")


class CosmosClient(BroadcasterMixin, AsyncBlockchainClient, ABC):
    key: MnemonicKey
    tx: TxApi

    def __init__(
        self,
        lcd_uri: str,
        rpc_http_uri: str,
        rpc_websocket_uri: str,
        grpc_uri: str,
        fee_denom: str,
        gas_prices: Coins.Input | None,
        gas_adjustment: Decimal,
        chain_id: str,
        allow_concurrent_pool_arbs: bool = False,
        *args,
        **kwargs,
    ):
        self.lcd_uri = lcd_uri
        self.rpc_http_uri = rpc_http_uri
        self.rpc_websocket_uri = rpc_websocket_uri
        self.grpc_uri = grpc_uri

        self.fee_denom = fee_denom
        self.gas_prices = Coins(gas_prices)
        self.gas_adjustment = gas_adjustment
        self.chain_id = chain_id

        self.broadcaster = BroadcasterApi(self, allow_concurrent_pool_arbs)
        self.ibc = IbcApi(self)
        self.mempool = MempoolApi(self)

        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(account={self.address}, chain_id={self.chain_id})"

    async def start(self):
        self.lcd_http_client = utils.ahttp.AsyncClient(base_url=self.lcd_uri)
        self.rpc_http_client = utils.ahttp.AsyncClient(base_url=self.rpc_http_uri)

        grpc_url, grpc_port = self.grpc_uri.split(":")
        self.grpc_channel = grpclib.client.Channel(grpc_url, int(grpc_port))
        self.grpc_service_tendermint = cosmos_tendermint_tb.ServiceStub(self.grpc_channel)
        self.grpc_query_auth = cosmos_auth_pb.QueryStub(self.grpc_channel)

        await asyncio.gather(self._init_lcd(), self._init_broadcaster_clients())
        await self._check_connections()

        self.mempool.start()
        self.tx.start()

        await super().start()

    async def _init_lcd(self):
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

    async def _check_connections(self):
        tasks = [
            self.lcd_http_client.check_connection("node_info"),
            self.rpc_http_client.check_connection("health"),
        ]
        results = await asyncio.gather(*tasks)
        assert all(results), "Connection error(s)"

        await asyncio.gather(
            *(conn.check_connection("node_info") for conn in self.broadcast_lcd_clients)
        )

    async def close(self):
        log.debug(f"Closing {self=}")
        self.mempool.stop()
        self.grpc_channel.close()
        await asyncio.gather(
            self.lcd_http_client.aclose(),
            self.rpc_http_client.aclose(),
            *(client.aclose() for client in self.broadcast_lcd_clients),
            self.lcd.session.close(),
        )
        self.started = False

    @abstractmethod
    async def get_balance(self, denom: str, address: AccAddress = None) -> CosmosTokenAmount:
        ...

    @abstractmethod
    async def get_all_balances(self, address: AccAddress = None) -> list[CosmosTokenAmount]:
        ...

    async def is_syncing(self) -> bool:
        res = await self.grpc_service_tendermint.get_syncing()
        return res.syncing

    async def get_latest_height(self) -> int:
        res = await self.grpc_service_tendermint.get_latest_block()
        return res.block.header.height

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
        res = await self.grpc_query_auth.account(address=address)
        return BaseAccount.from_proto_bytes(res.account.value)

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

    async def loop_latest_height(self) -> AsyncIterable[int]:
        async for height in utils_rpc.loop_latest_height(self.rpc_websocket_uri):
            yield height

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
                        "from": "cosmos1.....",
                        "to": "cosmos1....",
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
