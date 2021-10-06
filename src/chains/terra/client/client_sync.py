from __future__ import annotations

import base64
import json
import logging
import time
import urllib.parse
from collections import defaultdict
from typing import Iterable

from terra_sdk.client.lcd import LCDClient
from terra_sdk.core import AccAddress, Coins
from terra_sdk.core.auth import TxLog
from terra_sdk.key.mnemonic import MnemonicKey

import auth_secrets
import configs
import utils
from exceptions import NotContract
from utils.cache import CacheGroup, ttl_cache

from ..core import BaseTerraClient, TerraTokenAmount
from ..denoms import UST
from .api_market import MarketApi
from .api_oracle import OracleApi
from .api_treasury import TreasuryApi
from .api_tx import TxApi

log = logging.getLogger(__name__)

TERRA_CONTRACT_QUERY_CACHE_SIZE = 10_000
CONTRACT_INFO_CACHE_TTL = 86400  # Contract info should not change; 24h ttl
TERRA_CODE_IDS = "resources/contracts/terra/{chain_id}/code_ids.json"


def _get_code_ids(chain_id: str) -> dict[str, int]:
    return json.load(open(TERRA_CODE_IDS.format(chain_id=chain_id)))


class TerraClient(BaseTerraClient):
    def __init__(
        self,
        hd_wallet: dict = None,
        lcd_uri: str = configs.TERRA_LCD_URI,
        fcd_uri: str = configs.TERRA_FCD_URI,
        chain_id: str = configs.TERRA_CHAIN_ID,
        fee_denom: str = UST.denom,
        gas_prices: Coins.Input = None,
        gas_adjustment: float = configs.TERRA_GAS_ADJUSTMENT,
        hd_wallet_index: int = 0,
        raise_on_syncing: bool = configs.RAISE_ON_SYNCING,
    ):
        self.lcd_uri = lcd_uri
        self.fcd_uri = fcd_uri
        self.chain_id = chain_id
        self.fee_denom = fee_denom

        self.market = MarketApi(self)
        self.oracle = OracleApi(self)
        self.treasury = TreasuryApi(self)
        self.tx = TxApi(self)

        hd_wallet = auth_secrets.hd_wallet() if hd_wallet is None else hd_wallet
        key = MnemonicKey(hd_wallet["mnemonic"], hd_wallet["account"], hd_wallet_index)
        self.key = key  # Set key before get_gas_prices() to avoid error with cache debugging

        gas_prices = self.tx.get_gas_prices() if gas_prices is None else gas_prices
        self.lcd = LCDClient(lcd_uri, chain_id, gas_prices, gas_adjustment)
        self.wallet = self.lcd.wallet(key)
        self.address = self.wallet.key.acc_address

        self.code_ids = _get_code_ids(self.chain_id)
        self.block = self.get_latest_block()
        super().__init__(raise_on_syncing)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(lcd_uri={self.lcd_uri}, chain_id={self.chain_id}, "
            f"account={self.key.acc_address})"
        )

    @property
    def syncing(self) -> bool:
        return self.lcd.tendermint.syncing()

    @ttl_cache(CacheGroup.TERRA, TERRA_CONTRACT_QUERY_CACHE_SIZE)
    def contract_query(self, contract_addr: AccAddress, query_msg: dict) -> dict:
        return self.lcd.wasm.contract_query(contract_addr, query_msg)

    @ttl_cache(CacheGroup.TERRA, TERRA_CONTRACT_QUERY_CACHE_SIZE, CONTRACT_INFO_CACHE_TTL)
    def contract_info(self, address: AccAddress) -> dict:
        # return self.lcd.wasm.contract_info(contract_addr)  # returns 500 on non-account addresses
        info = self.fcd_get(f"v1/wasm/contract/{address}")
        if info is None:
            raise NotContract
        return info

    def fcd_get(self, path: str, **kwargs) -> dict:
        url = urllib.parse.urljoin(self.fcd_uri, path)
        res = utils.http.get(url, **kwargs)
        return res.json()

    def fcd_post(self, path: str, **kwargs) -> dict:
        url = urllib.parse.urljoin(self.fcd_uri, path)
        res = utils.http.get(url, **kwargs)
        return res.json()

    def get_bank(self, denoms: list[str] = None, address: str = None) -> list[TerraTokenAmount]:
        address = self.address if address is None else address
        coins_balance = self.lcd.bank.balance(address)
        return [
            TerraTokenAmount.from_coin(c)
            for c in coins_balance
            if denoms is None or c.denom in denoms
        ]

    @staticmethod
    def encode_msg(msg: dict) -> str:
        bytes_json = json.dumps(msg, separators=(",", ":")).encode("utf-8")
        return base64.b64encode(bytes_json).decode("ascii")

    def get_latest_block(self) -> int:
        return int(self.lcd.tendermint.block_info()["block"]["header"]["height"])

    def wait_next_block(self) -> Iterable[int]:
        while True:
            new_block = self.get_latest_block()
            block_diff = new_block - self.block
            if block_diff > 0:
                self.block = new_block
                log.debug(f"New block: {self.block}")
                if block_diff > 1:
                    log.warning(f"More than one block passed since last iteration ({block_diff})")
                yield self.block
            time.sleep(configs.TERRA_POLL_INTERVAL)

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
