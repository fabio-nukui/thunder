from decimal import Decimal
from enum import Enum

from utils.cache import CacheGroup, ttl_cache

from ..denoms import LUNA
from ..token import TerraNativeToken, TerraTokenAmount
from .base_api import Api

TERRA_TAX_CACHE_TTL = 7200


class TaxPayer(str, Enum):
    account = "account"
    contract = "contract"


class TreasuryApi(Api):
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=TERRA_TAX_CACHE_TTL)
    async def get_tax_rate(self) -> Decimal:
        return Decimal(str(await self.client.lcd.treasury.tax_rate()))

    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=TERRA_TAX_CACHE_TTL)
    async def get_tax_caps(self) -> dict[TerraNativeToken, TerraTokenAmount]:
        res = await self.client.lcd_http_client.get("/terra/treasury/v1beta1/tax_caps")
        caps = {}
        for cap in res.json()["tax_caps"]:
            token = TerraNativeToken(cap["denom"])
            caps[token] = token.to_amount(int_amount=cap["tax_cap"])
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
