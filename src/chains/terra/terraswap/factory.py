from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any, Iterable, Tuple, TypeVar

from terra_sdk.core.strings import AccAddress
from terra_sdk.exceptions import LCDResponseError

from exceptions import NotContract

from ..client import TerraClient
from .liquidity_pair import LiquidityPair
from .router import Router
from .utils import pair_tokens_from_data

_FactoryT = TypeVar("_FactoryT", bound="Factory")

log = logging.getLogger(__name__)

_FEES = {
    "terra154jt8ppucvvakvqa5fyfjdflsu6v83j4ckjfq3": Decimal("0.00300001"),  # LOOP_LOOPR
    "terra1dw5j23l6nwge69z0enemutfmyc93c36aqnzjj5": Decimal("0.00300001"),  # LOOPR_UST
}


def _get_fee_rate(contract_addr: str) -> Decimal | None:
    return _FEES.get(contract_addr)


class Factory:
    client: TerraClient
    addresses: dict[str, Any]
    name: str
    contract_addr: AccAddress
    pair_code_id: int
    lp_token_code_id: int

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.contract_addr})"

    @classmethod
    async def new(
        cls: type[_FactoryT],
        client: TerraClient,
        addresses: dict,
        name: str,
    ) -> _FactoryT:
        self = super().__new__(cls)
        self.client = client
        self.addresses = addresses
        self.name = name
        self.contract_addr = self.addresses["factory"]

        config = await client.contract_query(self.contract_addr, {"config": {}})
        self.pair_code_id = config["pair_code_id"]
        self.lp_token_code_id = config["token_code_id"]

        return self

    async def fetch_all_pair_infos(self) -> list[dict]:
        infos = []
        query_params = {}
        while True:
            page = await self.client.contract_query(self.contract_addr, {"pairs": query_params})
            data = page["pairs"]
            if not data:
                return infos
            infos.extend(data)
            query_params = {"start_after": data[-1]["asset_infos"]}

    async def generate_addresses_dict(self) -> dict[str, str | dict[str, str]]:
        pair_infos = await self.fetch_all_pair_infos()
        addresses: dict[str, Any] = {"factory": self.contract_addr, "pairs": {}}
        for info in pair_infos:
            try:
                tokens = await pair_tokens_from_data(info["asset_infos"], self.client)
            except NotImplementedError:  # Wrongly configured native token
                continue
            except NotContract:  # One or more of the tokens were not implemented
                continue
            except LCDResponseError as e:
                log.info(f"Error querying {info['contract_addr']}: {e.message}")
                continue
            pair_symbol = "-".join(token.repr_symbol for token in tokens)
            if pair_symbol in addresses["pairs"]:
                log.info(f"{pair_symbol=}, address={info['contract_addr']} already in pairs")
            else:
                addresses["pairs"][pair_symbol] = info["contract_addr"]
        return addresses

    async def get_pairs(self, pairs_names: Iterable[str]) -> Tuple[LiquidityPair, ...]:
        return await asyncio.gather(*(self.get_pair(pair) for pair in pairs_names))

    async def get_pair(self, pair_name: str) -> LiquidityPair:
        try:
            contract_addr = self.addresses["pairs"][pair_name]
        except KeyError:
            raise Exception(f"{self}: {pair_name} not in pairs addresses")
        return await LiquidityPair.new(
            contract_addr,
            self.client,
            fee_rate=_get_fee_rate(contract_addr),
            factory_name=self.name,
        )

    def get_router(self, liquidity_pairs: Iterable[LiquidityPair]) -> Router:
        if "router" not in self.addresses:
            raise Exception(f"{self}: no router address")
        return Router(self.addresses["router"], liquidity_pairs, self.client)

    async def is_pair(self, contract_addr: AccAddress) -> bool:
        try:
            info = await self.client.contract_info(contract_addr)
        except NotContract:
            return False
        return int(info["code_id"]) == self.pair_code_id

    async def is_lp_token(self, contract_addr: AccAddress) -> bool:
        try:
            info = await self.client.contract_info(contract_addr)
        except NotContract:
            return False
        return int(info["code_id"]) == self.lp_token_code_id
