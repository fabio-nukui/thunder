from decimal import Decimal
from enum import Enum

import utils
from utils.cache import CacheGroup, ttl_cache

from ...core import BaseTerraClient, TerraNativeToken, TerraToken, TerraTokenAmount

TERRA_TAX_CACHE_TTL = 7200


class TaxPayer(str, Enum):
    account = 'account'
    contract = 'contract'


class TreasuryApi:
    def __init__(self, client: BaseTerraClient):
        self.client = client

    @property
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=TERRA_TAX_CACHE_TTL)
    def tax_rate(self) -> Decimal:
        return Decimal(str(self.client.lcd.treasury.tax_rate()))

    @property
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=TERRA_TAX_CACHE_TTL)
    def tax_caps(self) -> dict[TerraToken, TerraTokenAmount]:
        res = utils.http.get(f'{self.client.lcd_uri}/terra/treasury/v1beta1/tax_caps')
        caps = {}
        for cap in res.json()['tax_caps']:
            token = TerraNativeToken(cap['denom'])
            caps[token] = TerraTokenAmount(token, int_amount=cap['tax_cap'])
        return caps

    def calculate_tax(
        self,
        amount: TerraTokenAmount,
        payer: TaxPayer = TaxPayer.contract,
    ) -> TerraTokenAmount:
        if amount.token not in self.tax_caps:
            return TerraTokenAmount(amount.token, 0)
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
