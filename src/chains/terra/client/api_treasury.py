from decimal import Decimal

from utils.cache import CacheGroup, ttl_cache

from ..core import BaseTreasuryApi, TaxPayer, TerraNativeToken, TerraToken, TerraTokenAmount

TERRA_TAX_CACHE_TTL = 7200


class TreasuryApi(BaseTreasuryApi):
    @property
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=TERRA_TAX_CACHE_TTL)
    def tax_rate(self) -> Decimal:
        return self.client.wait(self.get_tax_rate())

    async def get_tax_rate(self) -> Decimal:
        return Decimal(str(await self.client.lcd.treasury.tax_rate()))

    @property
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=TERRA_TAX_CACHE_TTL)
    def tax_caps(self) -> dict[TerraToken, TerraTokenAmount]:
        return self.client.wait(self.get_tax_caps())

    async def get_tax_caps(self) -> dict[TerraToken, TerraTokenAmount]:
        res = await self.client.lcd_http_client.get("/terra/treasury/v1beta1/tax_caps")
        caps = {}
        for cap in res.json()["tax_caps"]:
            token = TerraNativeToken(cap["denom"])
            caps[token] = token.to_amount(int_amount=cap["tax_cap"])
        return caps

    def calculate_tax(
        self,
        amount: TerraTokenAmount,
        payer: TaxPayer = TaxPayer.contract,
    ) -> TerraTokenAmount:
        if amount.token not in self.tax_caps:
            return amount.token.to_amount(0)
        if payer == TaxPayer.account:
            effective_rate = self.tax_rate
        else:
            effective_rate = self.tax_rate / (1 + self.tax_rate)
        return min(amount * effective_rate, self.tax_caps[amount.token])

    def deduct_tax(
        self,
        amount: TerraTokenAmount,
        payer: TaxPayer = TaxPayer.contract,
    ) -> TerraTokenAmount:
        return amount - self.calculate_tax(amount, payer)
