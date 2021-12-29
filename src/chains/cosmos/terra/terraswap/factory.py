from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Iterable, TypeVar

import grpclib
from cosmos_sdk.core import AccAddress
from cosmos_sdk.exceptions import LCDResponseError

from chains.cosmos.terra.token import TerraNativeToken
from exceptions import NotContract

from ...token import check_cw20_whitelist, get_cw20_whitelist
from .liquidity_pair import (
    ROUTER_SWAP_ACTION,
    LiquidityPair,
    RouterNativeLiquidityPair,
    pair_tokens_from_data,
)
from .router import Router, RouterLiquidityPair

if TYPE_CHECKING:
    from ..client import TerraClient

_FactoryT = TypeVar("_FactoryT", bound="Factory")

log = logging.getLogger(__name__)

_FEES: dict[str, Decimal] = {
    # "terra154jt8ppucvvakvqa5fyfjdflsu6v83j4ckjfq3": Decimal("0.00300001"),  # ldx LOOP-LOOPR
    # "terra1dw5j23l6nwge69z0enemutfmyc93c36aqnzjj5": Decimal("0.00300001"),  # ldx LOOPR-UST
    # "terra1kh2g4fnhvqtnwwpqa84eywn72ve9vdkp5chhlx": Decimal("0.00300050"),  # tsw ALTE-LUNA
    # "terra163pkeeuwxzr0yhndf8xd2jprm9hrtk59xf7nqf": Decimal("0.00300010"),  # tsw Psi-UST
    # "terra14zhkur7l7ut7tx6kvj28fp5q982lrqns59mnp3": Decimal("0.00300010"),  # tsw Psi-nETH
}


def _get_fee_rate(contract_addr: str) -> Decimal | None:
    return _FEES.get(contract_addr)


class Factory:
    client: TerraClient
    name: str | None
    contract_addr: AccAddress
    router_address: AccAddress | None
    router_swap_action = ROUTER_SWAP_ACTION
    pairs_addresses: dict[str, AccAddress]
    assert_limit_order_address: AccAddress | None
    pair_code_id: int
    cw20_token_code_id: int

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.contract_addr})"

    @classmethod
    async def new(
        cls: type[_FactoryT],
        client: TerraClient,
        addresses: dict,
        name: str = None,
    ) -> _FactoryT:
        self = super().__new__(cls)
        self.client = client
        self.name = name
        self.contract_addr = addresses["factory"]
        self.router_address = addresses.get("router")
        self.pairs_addresses = addresses["pairs"]
        self.assert_limit_order_address = addresses.get("assert_limit_order")

        config = await client.contract_query(self.contract_addr, {"config": {}})
        self.pair_code_id = config["pair_code_id"]
        self.cw20_token_code_id = config["token_code_id"]

        return self

    async def fetch_all_pair_infos(self) -> list[dict]:
        infos: list[dict] = []
        query_params: dict[str, Any] = {}
        while True:
            page = await self.client.contract_query(self.contract_addr, {"pairs": query_params})
            data: list[dict] = page["pairs"]
            if not data:
                return infos
            infos.extend(data)
            query_params = {"start_after": data[-1]["asset_infos"]}

    async def generate_addresses_dict(
        self,
        recursive: bool = True,
        router_address: str = None,
        assert_limit_order_address: str = None,
    ) -> dict[str, str | dict[str, str]]:
        pair_infos = await self.fetch_all_pair_infos()
        addresses: dict[str, Any] = {"factory": self.contract_addr, "pairs": {}}
        if router_address is not None:
            addresses["router"] = router_address
        if assert_limit_order_address is not None:
            addresses["assert_limit_order"] = assert_limit_order_address
        cw20_whitelist = get_cw20_whitelist(self.client.chain_id)
        for info in pair_infos:
            try:
                if recursive:
                    tokens = await pair_tokens_from_data(
                        info["asset_infos"],
                        self.client,
                        self.pair_code_id,
                        self.cw20_token_code_id,
                    )
                else:
                    tokens = await pair_tokens_from_data(info["asset_infos"], self.client)
            except NotImplementedError:  # Wrongly configured native token
                continue
            except NotContract:  # One or more of the tokens were not implemented
                continue
            except (LCDResponseError, grpclib.GRPCError) as e:
                status = e.response.status if isinstance(e, LCDResponseError) else e.status
                log.debug(f"Error querying {info['contract_addr']}: {status=} {e.message}")
                continue
            if not all(check_cw20_whitelist(token, cw20_whitelist) for token in tokens):
                log.debug(f"Rejected {info['contract_addr']}: one of {tokens} not in whitelist")
                continue
            pair_symbol = "-".join(f"[{token.repr_symbol}]" for token in tokens)
            if pair_symbol in addresses["pairs"]:
                log.debug(f"{pair_symbol=}, address={info['contract_addr']} already in pairs")
            else:
                addresses["pairs"][pair_symbol] = info["contract_addr"]
        addresses["pairs"] = dict(sorted(addresses["pairs"].items()))
        return addresses

    async def get_pairs(self, pairs_names: Iterable[str]) -> tuple[LiquidityPair, ...]:
        return await asyncio.gather(*(self.get_pair(pair) for pair in pairs_names))  # type: ignore # noqa: E501

    async def get_pair(self, pair_name: str, check_liquidity: bool = True) -> LiquidityPair:
        try:
            contract_addr = self.pairs_addresses[pair_name]
        except KeyError:
            raise Exception(f"{self}: {pair_name} not in pairs addresses")
        assert await self.is_pair(contract_addr)
        return await LiquidityPair.new(
            contract_addr,
            self.client,
            fee_rate=_get_fee_rate(contract_addr),
            factory_name=self.name,
            factory_address=self.contract_addr,
            router_address=self.router_address,
            assert_limit_order_address=self.assert_limit_order_address,
            check_liquidity=check_liquidity,
        )

    def get_native_pair(
        self, tokens: tuple[TerraNativeToken, TerraNativeToken]
    ) -> RouterNativeLiquidityPair:
        if self.router_address is None:
            raise Exception("Cannot create native pair if router_addres is None")
        return RouterNativeLiquidityPair(
            self.client,
            tokens,
            self.contract_addr,
            self.router_address,
            self.router_swap_action,
            self.assert_limit_order_address,
        )

    def get_router(self, liquidity_pairs: Iterable[RouterLiquidityPair]) -> Router:
        if not self.router_address:
            raise Exception(f"{self}: no router address")
        return Router(self.router_address, liquidity_pairs, self.client)

    async def is_pair(self, contract_addr: AccAddress) -> bool:
        try:
            info = await self.client.contract_info(contract_addr)
        except NotContract:
            return False
        return int(info["code_id"]) == self.pair_code_id

    async def is_cw20_token(self, contract_addr: AccAddress) -> bool:
        try:
            info = await self.client.contract_info(contract_addr)
        except NotContract:
            return False
        return int(info["code_id"]) == self.cw20_token_code_id
