import asyncio
from typing import Iterable, Tuple, TypeVar

from terra_sdk.core.strings import AccAddress

from exceptions import NotContract

from ..client import TerraClient
from .liquidity_pair import LiquidityPair
from .router import Router

_FactoryT = TypeVar("_FactoryT", bound="Factory")


class Factory:
    client: TerraClient
    contract_addr: str
    pair_code_id: int
    lp_token_code_id: int
    addresses: dict

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.contract_addr})"

    @classmethod
    async def new(cls: type[_FactoryT], client: TerraClient, addresses: dict) -> _FactoryT:
        self = super().__new__(cls)
        self.addresses = addresses
        self.contract_addr = self.addresses["factory"]
        self.client = client

        config = await client.contract_query(self.contract_addr, {"config": {}})
        self.pair_code_id = config["pair_code_id"]
        self.lp_token_code_id = config["token_code_id"]

        return self

    async def get_pairs(self, pairs_names: Iterable[str]) -> Tuple[LiquidityPair, ...]:
        return await asyncio.gather(*(self.get_pair(pair) for pair in pairs_names))

    async def get_pair(self, pair_name: str) -> LiquidityPair:
        try:
            contract_addr = self.addresses["pairs"][pair_name]
        except KeyError:
            raise Exception(f"{self}: {pair_name} not in pairs addresses")
        return await LiquidityPair.new(contract_addr, self.client)

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
