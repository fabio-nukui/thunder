from __future__ import annotations

import base64
import json
import logging
from collections import defaultdict
from decimal import Decimal

import cosmos_proto.cosmos.bank.v1beta1 as cosmos_bank_pb
import cosmos_proto.terra.wasm.v1beta1 as terra_wasm_pb
from cosmos_sdk.core import AccAddress, Coins
from cosmos_sdk.core.auth import TxLog
from cosmos_sdk.core.auth.data import BaseAccount
from cosmos_sdk.key.mnemonic import MnemonicKey
from grpclib.const import Status as GRPCStatus
from grpclib.exceptions import GRPCError

import auth_secrets
import configs
import utils
from exceptions import NotContract
from utils.cache import CacheGroup, ttl_cache

from ...client import CosmosClient
from ..denoms import UST
from ..token import TerraNativeToken, TerraTokenAmount
from .api_market import MarketApi
from .api_oracle import OracleApi
from .api_treasury import TreasuryApi
from .api_tx import TxApi

log = logging.getLogger(__name__)


_CONTRACT_QUERY_CACHE_SIZE = 10_000
_CONTRACT_INFO_CACHE_TTL = 86400  # Contract info should not change; 24h ttl


class TerraClient(CosmosClient):
    tx: TxApi  # Override superclass' tx attribute

    def __init__(
        self,
        lcd_uri: str = configs.TERRA_LCD_URI,
        fcd_uri: str = configs.TERRA_FCD_URI,
        rpc_http_uri: str = configs.TERRA_RPC_HTTP_URI,
        rpc_websocket_uri: str = configs.TERRA_RPC_WEBSOCKET_URI,
        grpc_uri: str = configs.TERRA_GRPC_URI,
        use_broadcaster: bool = configs.TERRA_USE_BROADCASTER,
        broadcaster_uris: list[str] = configs.TERRA_BROADCASTER_URIS,
        broadcast_lcd_uris: list[str] = configs.TERRA_BROADCAST_LCD_URIS,
        chain_id: str = configs.TERRA_CHAIN_ID,
        allow_concurrent_pool_arbs: bool = False,
        fee_denom: str = UST.denom,
        gas_prices: Coins.Input = None,
        gas_adjustment: Decimal = configs.TERRA_GAS_ADJUSTMENT,
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

        self.fcd_uri = fcd_uri
        hd_wallet = auth_secrets.hd_wallet() if hd_wallet is None else hd_wallet
        self.key = MnemonicKey(hd_wallet["mnemonic"], hd_wallet["account"], hd_wallet_index)

        self.market = MarketApi(self)
        self.oracle = OracleApi(self)
        self.treasury = TreasuryApi(self)
        self.tx = TxApi(self)

    async def start(self):
        self.fcd_client = utils.ahttp.AsyncClient(base_url=self.fcd_uri)
        assert await self.fcd_client.check_connection("node_info")

        await super().start()
        try:
            if not self.gas_prices:
                self.gas_prices = self.lcd.gas_prices = await self.tx.get_gas_prices()

            await self.update_active_broadcaster()
        except Exception as e:
            log.warning(f"{self}: Error in initialization: {e!r}")
            self.started = False

        self.grpc_bank = cosmos_bank_pb.QueryStub(self.grpc_channel)
        self.grpc_wasm = terra_wasm_pb.QueryStub(self.grpc_channel)

        self.market.start()
        self.oracle.start()
        self.treasury.start()

    async def close(self):
        await self.fcd_client.aclose()
        await super().close()

    @ttl_cache(CacheGroup.TERRA, _CONTRACT_QUERY_CACHE_SIZE)
    async def contract_query(self, contract_addr: AccAddress, query_msg: dict) -> dict:
        try:
            res = await self.grpc_wasm.contract_store(
                contract_address=contract_addr, query_msg=json.dumps(query_msg).encode("utf-8")
            )
        except GRPCError as e:
            if (
                e.status == GRPCStatus.NOT_FOUND
                or e.status == GRPCStatus.INTERNAL
                and "not found" in str(e.message)
            ):
                raise NotContract
            raise
        return json.loads(res.query_result)

    @ttl_cache(CacheGroup.TERRA, _CONTRACT_QUERY_CACHE_SIZE, _CONTRACT_INFO_CACHE_TTL)
    async def contract_info(self, address: AccAddress) -> dict:
        try:
            res = await self.grpc_wasm.contract_info(contract_address=address)
        except GRPCError as e:
            if e.status == GRPCStatus.NOT_FOUND or "not found" in str(e.message):
                raise NotContract
            raise
        data = res.contract_info.to_dict()
        return {
            "code_id": int(data.pop("codeId")),
            "init_msg": json.loads(base64.b64decode(data.pop("initMsg"))),
            **data,
        }

    @ttl_cache(CacheGroup.TERRA)
    async def get_balance(self, denom: str, address: AccAddress = None) -> TerraTokenAmount:
        address = self.address if address is None else address
        res = await self.grpc_bank.balance(address=address, denom=denom)
        return TerraNativeToken(res.balance.denom).to_amount(int_amount=res.balance.amount)

    @ttl_cache(CacheGroup.TERRA)
    async def get_all_balances(self, address: AccAddress = None) -> list[TerraTokenAmount]:
        address = self.address if address is None else address
        res = await self.grpc_bank.all_balances(address=address)
        if res.pagination.next_key:
            raise NotImplementedError("not implemented for paginated results")
        return [TerraNativeToken(c.denom).to_amount(int_amount=c.amount) for c in res.balances]

    @ttl_cache(CacheGroup.TERRA)
    async def get_account_data(self, address: AccAddress = None) -> BaseAccount:
        return await super().get_account_data(address)

    @staticmethod
    def get_coin_balance_changes(
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
