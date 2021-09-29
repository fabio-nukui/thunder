from __future__ import annotations

import logging
from decimal import Decimal

from terra_sdk.core.wasm import MsgExecuteContract

from chains.terra.client import TerraClient
from exceptions import InsufficientLiquidity

from .core import CW20Token, TerraNativeToken, TerraToken, TerraTokenAmount
from .utils import encode_msg

log = logging.getLogger(__name__)

FEE = Decimal('0.003')
TERRASWAP_CODE_ID_KEY = 'terraswap_pair'


class NotTerraswapPair(Exception):
    pass


def _token_to_data(token: TerraToken) -> dict:
    if isinstance(token, TerraNativeToken):
        return {'native_token': {'denom': token.denom}}
    return {'token': {'contract_addr': token.contract_addr}}


def _token_amount_to_data(token_amount: TerraTokenAmount) -> dict:
    return {
        'info': _token_to_data(token_amount.token),
        'amount': str(token_amount.raw_amount)
    }


def _is_terraswap_pool(contract_addr: str, client: TerraClient) -> bool:
    minter_code_id = int(client.contract_info(contract_addr)['code_id'])
    return minter_code_id == client.code_ids[TERRASWAP_CODE_ID_KEY]


class TerraswapLiquidityPair:
    def __init__(self, contract_addr: str, client: TerraClient):
        if not _is_terraswap_pool(contract_addr, client):
            raise NotTerraswapPair
        self.contract_addr = contract_addr
        self.client = client

        pair_data = self.client.contract_query(self.contract_addr, {'pair': {}})
        self.tokens = self._pair_tokens_from_data(pair_data['asset_infos'])
        self._reserves = TerraTokenAmount(self.tokens[0]), TerraTokenAmount(self.tokens[1])
        self.lp_token = TerraswapLPToken.from_pool(pair_data['liquidity_token'], self)

    def __repr__(self) -> str:
        return \
            f'{self.__class__.__name__}({self.tokens[0].repr_symbol}/{self.tokens[1].repr_symbol})'

    @property
    def reserves(self) -> tuple[TerraTokenAmount, TerraTokenAmount]:
        self._update_reserves()
        return self._reserves

    def _get_assets_data(self) -> list[dict]:
        response = self.client.contract_query(self.contract_addr, {'pool': {}})
        return response['assets']

    def _pair_tokens_from_data(self, asset_infos: list[dict]) -> tuple[TerraToken, TerraToken]:
        return self._token_from_data(asset_infos[0]), self._token_from_data(asset_infos[1])

    def _token_from_data(self, asset_info: dict) -> TerraToken:
        if 'native_token' in asset_info:
            return TerraNativeToken(asset_info['native_token']['denom'])
        if 'token' in asset_info:
            contract_addr: str = asset_info['token']['contract_addr']
            try:
                return TerraswapLPToken.from_contract(contract_addr, self.client)
            except NotTerraswapPair:
                return CW20Token.from_contract(contract_addr, self.client)
        raise TypeError(f'Unexpected data format: {asset_info}')

    def _update_reserves(self):
        data = self._get_assets_data()
        for reserve, asset_data in zip(self._reserves, data):
            reserve.raw_amount = asset_data['amount']

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

    def get_amount_out(
        self,
        amount_in: TerraTokenAmount,
        deduct_tax: bool = True,
    ) -> TerraTokenAmount:
        reserve_in, reserve_out = self._get_in_out_reserves(amount_in=amount_in)

        numerator = reserve_out.amount * amount_in.amount
        denominator = reserve_in.amount + amount_in.amount

        amount_out = numerator / denominator * (1 - FEE)
        result = TerraTokenAmount(reserve_out.token, amount_out)

        return self.client.deduct_tax(result) if deduct_tax else result

    def build_swap_msg(
        self,
        sender: str,
        amount_in: TerraTokenAmount,
        min_out: TerraTokenAmount,
    ) -> MsgExecuteContract:
        belief_price = amount_in.raw_amount / (min_out.raw_amount - 1)
        swap_msg = {
            'belief_price': f'{belief_price:.18f}',
            'max_spread': '0.0'
        }
        if isinstance(token_in := amount_in.token, CW20Token):
            execute_msg = {
                'send': {
                    'contract': self.contract_addr,
                    'amount': str(amount_in.raw_amount),
                    'msg': encode_msg({'swap': swap_msg})
                }
            }
            contract = token_in.contract_addr
        else:
            execute_msg = {
                'swap': {
                    'offer_asset': _token_amount_to_data(amount_in),
                    **swap_msg
                }
            }
            contract = self.contract_addr
        coins = [amount_in.to_coin()] if isinstance(amount_in.token, TerraNativeToken) else []

        return MsgExecuteContract(
            sender=sender,
            contract=contract,
            execute_msg=execute_msg,
            coins=coins
        )

    def swap(
        self,
        client: TerraClient,
        amount_in: TerraTokenAmount,
        min_out: TerraTokenAmount,
    ) -> str:
        msg = self.build_swap_msg(client.address, amount_in, min_out)
        return client.execute_tx([msg])


class TerraswapLPToken(CW20Token):
    pair_tokens: tuple[TerraToken, TerraToken]

    @classmethod
    def from_pool(cls, contract_addr: str, pool: TerraswapLiquidityPair) -> TerraswapLPToken:
        self = super().from_contract(contract_addr, pool.client)
        self.pair_tokens = pool.tokens
        return self

    @classmethod
    def from_contract(cls, contract_addr: str, client: TerraClient) -> TerraswapLPToken:
        minter_addr = client.contract_query(contract_addr, {'minter': {}})['minter']
        return TerraswapLiquidityPair(minter_addr, client).lp_token

    @property
    def repr_symbol(self):
        return '-'.join(t.repr_symbol for t in self.pair_tokens)
