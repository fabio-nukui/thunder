from __future__ import annotations

from terra_sdk.core import Dec

from chains.terra.client import TerraClient

from .core import CW20Token, NativeToken, TokenAmount

FEE = Dec('0.003')


class TerraswapLiquidityPair:
    def __init__(self, contract_addr: str, client: TerraClient):
        self.contract_addr = contract_addr
        self.client = client

        data = self._get_assets_data()
        self.tokens = tuple(self._token_from_data(asset['info']) for asset in data)

        self._reserves = tuple(
            TokenAmount(token, asset_data['amount'], decimalize=True)
            for token, asset_data
            in zip(self.tokens, data)
        )

    @property
    def reserves(self) -> tuple[TokenAmount, ...]:
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
