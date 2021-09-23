from __future__ import annotations

from terra_sdk.core import Dec

from chains.terra.client import TerraClient
from exceptions import InsufficientLiquidity

from .core import CW20Token, NativeToken, TokenAmount

FEE = Dec('0.003')


class TerraswapLiquidityPair:
    def __init__(self, contract_addr: str, client: TerraClient):
        self.contract_addr = contract_addr
        self.client = client

        data = self._get_assets_data()
        self.tokens = self._token_from_data(data[0]['info']), self._token_from_data(data[1]['info'])

        self._reserves = (
            TokenAmount(self.tokens[0], data[0]['amount'], decimalize=True),
            TokenAmount(self.tokens[1], data[1]['amount'], decimalize=True)
        )

    @property
    def reserves(self) -> tuple[TokenAmount, TokenAmount]:
        self._update_reserves()
        return self._reserves

    def _get_assets_data(self) -> list[dict]:
        response = self.client.contract_query(self.contract_addr, {'pool': {}})
        return response['assets']

    def _token_from_data(self, asset_info: dict) -> NativeToken | CW20Token:
        if 'native_token' in asset_info:
            return NativeToken(asset_info['native_token']['denom'])
        if 'token' in asset_info:
            return CW20Token.from_contract(asset_info['token']['contract_addr'], self.client)
        raise TypeError(f'Unexpected data format: {asset_info}')

    def _update_reserves(self):
        data = self._get_assets_data()
        for reserve, asset_data in zip(self._reserves, data):
            reserve.update_amount(asset_data['amount'], decimalize=True)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.tokens[0].symbol}-{self.tokens[1].symbol})'

    def _get_in_out_reserves(
        self,
        amount_in: TokenAmount = None,
        amount_out: TokenAmount = None
    ) -> tuple[TokenAmount, TokenAmount]:
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

    def get_amount_out(self, amount_in: TokenAmount) -> TokenAmount:
        reserve_in, reserve_out = self._get_in_out_reserves(amount_in=amount_in)

        numerator = reserve_out.amount * amount_in.amount
        denominator = reserve_in.amount + amount_in.amount

        amount_out = numerator / denominator * (1 - FEE)

        return TokenAmount(reserve_out.token, amount_out)
