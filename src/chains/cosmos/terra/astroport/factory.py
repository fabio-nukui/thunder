from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any, TypeVar

from cosmos_sdk.core import AccAddress

from exceptions import NotContract

from ...token import check_cw20_whitelist, get_cw20_whitelist
from ..terraswap.factory import Factory as TerraswapFactory
from ..terraswap.liquidity_pair import pair_tokens_from_data
from .liquidity_pair import ROUTER_SWAP_ACTION, LiquidityPair, PairConfig

if TYPE_CHECKING:
    from ..client import TerraClient

log = logging.getLogger(__name__)

_FactoryT = TypeVar("_FactoryT", bound="Factory")


class Factory(TerraswapFactory):
    pair_configs: list[PairConfig]
    router_swap_action = ROUTER_SWAP_ACTION

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
        self.pair_configs = [
            PairConfig(
                pair_config["pair_type"],
                pair_config["code_id"],
                Decimal(pair_config["total_fee_bps"]) / 10_000,
                pair_config["is_disabled"],
            )
            for pair_config in config["pair_configs"]
        ]
        return self

    async def generate_addresses_dict(
        self,
        recursive: bool = False,
        router_address: str = None,
        assert_limit_order_address: str = None,
    ) -> dict[str, str | dict[str, str]]:
        if recursive:
            raise NotImplementedError
        pair_infos = await self.fetch_all_pair_infos()
        addresses: dict[str, Any] = {"factory": self.contract_addr, "pairs": {}}
        if router_address is not None:
            addresses["router"] = router_address
        if assert_limit_order_address is not None:
            addresses["assert_limit_order"] = assert_limit_order_address
        cw20_whitelist = get_cw20_whitelist(self.client.chain_id)
        for info in pair_infos:
            tokens = await pair_tokens_from_data(info["asset_infos"], self.client)
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

    async def get_pair(self, pair_name: str, check_liquidity: bool = True) -> LiquidityPair:
        try:
            contract_addr = self.pairs_addresses[pair_name]
        except KeyError:
            raise Exception(f"{self}: {pair_name} not in pairs addresses")
        assert await self.is_pair(contract_addr)
        return await LiquidityPair.new(
            contract_addr,
            self.client,
            factory_name=self.name,
            factory_address=self.contract_addr,
            router_address=self.router_address,
            check_liquidity=check_liquidity,
            pair_configs=self.pair_configs,
        )

    async def is_pair(self, contract_addr: AccAddress) -> bool:
        try:
            info = await self.client.contract_info(contract_addr)
        except NotContract:
            return False
        code_id = int(info["code_id"])
        return any(code_id == c.code_id for c in self.pair_configs)
