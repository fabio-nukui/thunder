from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING

from cosmos_proto.terra.treasury.v1beta1 import QueryStub

from utils.cache import CacheGroup, ttl_cache

from ...client.base_api import Api
from ..denoms import LUNA
from ..token import TerraNativeToken, TerraTokenAmount

if TYPE_CHECKING:
    from .async_client import TerraClient  # noqa: F401

_TERRA_TAX_CACHE_TTL = 3600
_PROTO_RECISION = 10 ** 18


class TaxPayer(str, Enum):
    account = "account"
    contract = "contract"


class TreasuryApi(Api["TerraClient"]):
    def start(self):
        self.grpc_query = QueryStub(self.client.grpc_channel)

    @ttl_cache(CacheGroup.TERRA, ttl=_TERRA_TAX_CACHE_TTL)
    async def get_tax_rate(self) -> Decimal:
        res = await self.grpc_query.tax_rate()
        return Decimal(res.tax_rate) / _PROTO_RECISION

    @ttl_cache(CacheGroup.TERRA, ttl=_TERRA_TAX_CACHE_TTL)
    async def get_tax_caps(self) -> dict[TerraNativeToken, TerraTokenAmount]:
        res = await self.grpc_query.tax_caps()
        caps: dict[TerraNativeToken, TerraTokenAmount] = {}
        for cap in res.tax_caps:
            token = TerraNativeToken(cap.denom)
            caps[token] = token.to_amount(int_amount=cap.tax_cap)
        return caps

    async def calculate_tax(
        self,
        amount: TerraTokenAmount,
        payer: TaxPayer = TaxPayer.contract,
    ) -> TerraTokenAmount:
        tax_caps = await self.get_tax_caps()
        if not isinstance(amount.token, TerraNativeToken) or amount.token == LUNA:
            return amount.token.to_amount(0)

        tax_rate = await self.get_tax_rate()
        if payer == TaxPayer.account:
            effective_rate = tax_rate
        else:
            effective_rate = tax_rate / (1 + tax_rate)
        return min(amount * effective_rate, tax_caps[amount.token])

    async def deduct_tax(
        self,
        amount: TerraTokenAmount,
        payer: TaxPayer = TaxPayer.contract,
    ) -> TerraTokenAmount:
        return amount - await self.calculate_tax(amount, payer)
