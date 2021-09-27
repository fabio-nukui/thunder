from __future__ import annotations

from decimal import Decimal

from chains.terra.client import TerraClient
from exceptions import InsufficientLiquidity

from .core import CW20Token, TerraNativeToken, TerraToken, TerraTokenAmount

FEE = Decimal('0.003')


class TerraswapLiquidityPair:
    def __init__(self, contract_addr: str, client: TerraClient):
        self.contract_addr = contract_addr
        self.client = client

        data = self._get_assets_data()
        self.tokens = self._token_from_data(data[0]['info']), self._token_from_data(data[1]['info'])

        self._reserves = (
            TerraTokenAmount(self.tokens[0], raw_amount=data[0]['amount']),
            TerraTokenAmount(self.tokens[1], raw_amount=data[1]['amount'])
        )

    @property
    def reserves(self) -> tuple[TerraTokenAmount, TerraTokenAmount]:
        self._update_reserves()
        return self._reserves

    def _get_assets_data(self) -> list[dict]:
        response = self.client.contract_query(self.contract_addr, {'pool': {}})
        return response['assets']

    def _token_from_data(self, asset_info: dict) -> TerraToken:
        if 'native_token' in asset_info:
            return TerraNativeToken(asset_info['native_token']['denom'])
        if 'token' in asset_info:
            return CW20Token.from_contract(asset_info['token']['contract_addr'], self.client)
        raise TypeError(f'Unexpected data format: {asset_info}')

    def _update_reserves(self):
        data = self._get_assets_data()
        for reserve, asset_data in zip(self._reserves, data):
            reserve.raw_amount = asset_data['amount']

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.tokens[0].symbol}-{self.tokens[1].symbol})'

    def _get_in_out_reserves(
        self,
        amount_in: TerraTokenAmount = None,
        amount_out: TerraTokenAmount = None
    ) -> tuple[TerraTokenAmount, TerraTokenAmount]:
        """Given an amount in and/or an amount out, checks for insuficient liquidity and return
        the reserves pair in order reserve_in, reserve_out"""
        assert amount_in is None or amount_in.token in self.tokens, 'amount_in not in pair'
        assert amount_out is None or amount_out.token in self.tokens, 'amount_out not in pair'

        if self.reserves[0] == 0 or self.reserves[1] == 0:
            raise InsufficientLiquidity
        if amount_in is not None:
            token_in = amount_in.token
        elif amount_out is not None:
            token_in = self.tokens[0] if amount_out.token == self.tokens[1] else self.tokens[1]
        else:
            raise Exception('At least one of token_in or token_out must be passed')

        if token_in == self.tokens[0]:
            reserve_in, reserve_out = self.reserves
        else:
            reserve_out, reserve_in = self.reserves
        if amount_out is not None and amount_out >= reserve_out:
            raise InsufficientLiquidity
        return reserve_in, reserve_out

    def get_amount_out(self, amount_in: TerraTokenAmount) -> TerraTokenAmount:
        reserve_in, reserve_out = self._get_in_out_reserves(amount_in=amount_in)

        numerator = reserve_out.amount * amount_in.amount
        denominator = reserve_in.amount + amount_in.amount

        amount_out = numerator / denominator * (1 - FEE)

        return TerraTokenAmount(reserve_out.token, amount_out)
