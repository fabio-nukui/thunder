from __future__ import annotations

import logging
from contextlib import contextmanager
from copy import copy
from decimal import Decimal

from terra_sdk.core.wasm import MsgExecuteContract

from chains.terra.client import TerraClient
from exceptions import InsufficientLiquidity

from .core import CW20Token, TerraNativeToken, TerraToken, TerraTokenAmount

log = logging.getLogger(__name__)

FEE = Decimal('0.003')
TERRASWAP_CODE_ID_KEY = 'terraswap_pair'
DEFAULT_ADD_LIQUIDITY_SLIPPAGE_TOLERANCE = Decimal(0.001)

AmountTuple = tuple[TerraTokenAmount, TerraTokenAmount]


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
        self.lp_token = TerraswapLPToken.from_pool(pair_data['liquidity_token'], self)

        self.stop_updates = False
        self._reserves = TerraTokenAmount(self.tokens[0]), TerraTokenAmount(self.tokens[1])

    def __repr__(self) -> str:
        return \
            f'{self.__class__.__name__}({self.tokens[0].repr_symbol}/{self.tokens[1].repr_symbol})'

    @property
    def reserves(self) -> AmountTuple:
        if not self.stop_updates:
            self._update_reserves()
        return self._reserves

    def _fix_amounts_order(self, amounts: AmountTuple) -> AmountTuple:
        if (amounts[1].token, amounts[0].token) == self.tokens:
            return amounts[1], amounts[0]
        if (amounts[0].token, amounts[1].token) == self.tokens:
            return amounts
        raise Exception('Tokens in amounts do not match reserves')

    @contextmanager
    def simulate_reserve_change(self, amounts: AmountTuple):
        amounts = self._fix_amounts_order(amounts)
        reserves = copy(self.reserves)
        stop_updates = self.stop_updates
        try:
            self.stop_updates = True
            self._reserves = reserves[0] + amounts[0], reserves[1] + amounts[1]
            yield
        finally:
            self._reserves = reserves
            self.stop_updates = stop_updates

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
        data = self.client.contract_query(self.contract_addr, {'pool': {}})['assets']
        for reserve, asset_data in zip(self._reserves, data):
            reserve.raw_amount = asset_data['amount']

    def get_price(self, token_quote: TerraNativeToken) -> Decimal:
        if token_quote in self.tokens:
            reference_token = token_quote
            exchange_rate = Decimal(1)
        else:
            for token in self.tokens:
                if isinstance(token, TerraNativeToken):
                    reference_token = token
                    break
            else:
                raise NotImplementedError('not implemented for pools without a native token')
            exchange_rate = self.client.get_exchange_rate(reference_token, token_quote)
        for reserve in self.reserves:
            if reserve.token == reference_token:
                amount_per_lp_token = reserve.amount / self.lp_token.get_supply(self.client).amount
                return amount_per_lp_token * exchange_rate * 2
        raise Exception  # Should never reach

    def _get_in_out_reserves(
        self,
        amount_in: TerraTokenAmount = None,
        amount_out: TerraTokenAmount = None
    ) -> AmountTuple:
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

    def get_swap_amounts(self, amount_in: TerraTokenAmount) -> dict[str, TerraTokenAmount]:
        reserve_in, reserve_out = self._get_in_out_reserves(amount_in=amount_in)

        numerator = reserve_out.amount * amount_in.amount
        denominator = reserve_in.amount + amount_in.amount
        amount_out = TerraTokenAmount(reserve_out.token, numerator / denominator)

        amount_out = amount_out - (fee := amount_out * FEE)
        amount_out = amount_out - (tax := self.client.calculate_tax(amount_out))

        return {
            'amount_out': amount_out,
            'fee': fee,
            'tax': tax,
            'pool_change': -amount_out + fee,
        }

    def get_swap_amount_out(self, amount_in: TerraTokenAmount) -> TerraTokenAmount:
        return self.get_swap_amounts(amount_in)['amount_out']

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
            contract = token_in.contract_addr
            execute_msg = {
                'send': {
                    'contract': self.contract_addr,
                    'amount': str(amount_in.raw_amount),
                    'msg': TerraClient.encode_msg({'swap': swap_msg})
                }
            }
            coins = []
        else:
            contract = self.contract_addr
            execute_msg = {
                'swap': {
                    'offer_asset': _token_amount_to_data(amount_in),
                    **swap_msg
                }
            }
            coins = [amount_in.to_coin()]

        return MsgExecuteContract(
            sender=sender,
            contract=contract,
            execute_msg=execute_msg,
            coins=coins
        )

    def op_swap(
        self,
        sender: str,
        amount_in: TerraTokenAmount,
        max_slippage: Decimal,
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        amount_out = self.get_swap_amount_out(amount_in)
        min_amount_out = amount_out * (1 - max_slippage)
        msg = self.build_swap_msg(sender, amount_in, min_amount_out)

        return amount_out, [msg]

    def get_remove_liquidity_amounts(self, amount_burn: TerraTokenAmount) -> dict[str, AmountTuple]:
        assert amount_burn.token == self.lp_token

        total_supply = self.lp_token.get_supply(self.client)
        share = amount_burn / total_supply
        amounts = self.reserves[0] * share, self.reserves[1] * share

        taxes = self.client.calculate_tax(amounts[0]), self.client.calculate_tax(amounts[1])
        amounts_out = amounts[0] - taxes[0], amounts[1] - taxes[1]
        return {
            'amounts_out': amounts_out,
            'taxes': taxes,
            'pool_change': (-amounts[0], -amounts[1]),
        }

    def get_remove_liquidity_amounts_out(self, amount_burn: TerraTokenAmount) -> AmountTuple:
        return self.get_remove_liquidity_amounts(amount_burn)['amounts_out']

    def build_remove_liquidity_msg(
        self,
        sender: str,
        amount_burn: TerraTokenAmount,
    ) -> MsgExecuteContract:
        assert amount_burn.token == self.lp_token
        execute_msg = {
            'send': {
                'amount': str(amount_burn.raw_amount),
                'contract': self.contract_addr,
                'msg': TerraClient.encode_msg({'withdraw_liquidity': {}})
            }
        }
        return MsgExecuteContract(
            sender=sender,
            contract=self.lp_token,
            execute_msg=execute_msg,
        )

    def op_remove_liquidity(
        self,
        sender: str,
        amount_burn: TerraTokenAmount,
    ) -> tuple[AmountTuple, list[MsgExecuteContract]]:
        amounts = self.get_remove_liquidity_amounts_out(amount_burn)
        msg_remove_liquidity = self.build_remove_liquidity_msg(sender, amount_burn)
        return amounts, [msg_remove_liquidity]

    def op_remove_single_side(
        self,
        sender: str,
        amount_burn: TerraTokenAmount,
        token_out: TerraToken,
        max_slippage: Decimal,
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        assert token_out in self.tokens
        amounts = self.get_remove_liquidity_amounts(amount_burn)
        msg_remove_liquidity = self.build_remove_liquidity_msg(sender, amount_burn)
        if token_out == self.tokens[0]:
            amount_keep, amount_swap = amounts['amounts_out']
        else:
            amount_swap, amount_keep = amounts['amounts_out']
        with self.simulate_reserve_change(amounts['pools_change']):
            amount_out, msgs_swap = self.op_swap(sender, amount_swap, max_slippage)
        return amount_keep + amount_out, [msg_remove_liquidity] + msgs_swap

    def _check_amounts_add_liquidity(
        self,
        amounts_in: AmountTuple,
        slippage_tolerance: Decimal = DEFAULT_ADD_LIQUIDITY_SLIPPAGE_TOLERANCE,
    ) -> AmountTuple:
        amounts_in = self._fix_amounts_order(amounts_in)
        amounts_ratio = Decimal(amounts_in[0].raw_amount / amounts_in[1].raw_amount)
        current_ratio = Decimal(self.reserves[0].raw_amount / self.reserves[1].raw_amount)
        assert abs(amounts_ratio / current_ratio - 1) < slippage_tolerance
        return amounts_in

    def get_add_liquidity_amount_out(
        self,
        amounts_in: AmountTuple,
        slippage_tolerance: Decimal = DEFAULT_ADD_LIQUIDITY_SLIPPAGE_TOLERANCE,
    ) -> TerraTokenAmount:
        amounts_in = self._check_amounts_add_liquidity(amounts_in, slippage_tolerance)
        add_ratio = min(amounts_in[0] / self.reserves[0], amounts_in[1] / self.reserves[1])
        return self.lp_token.get_supply(self.client) * add_ratio

    def build_add_liquity_msgs(
        self,
        sender: str,
        amounts_in: AmountTuple,
        slippage_tolerance: Decimal = DEFAULT_ADD_LIQUIDITY_SLIPPAGE_TOLERANCE,
    ) -> list[MsgExecuteContract]:
        amounts_in = self._check_amounts_add_liquidity(amounts_in, slippage_tolerance)
        msgs = []
        for amount in amounts_in:
            if not amount.has_allowance(self.client, self.contract_addr, sender):
                msgs.append(amount.build_msg_increase_allowance(self.contract_addr, sender))
        execute_msg = {
            'provide_liquidity': {
                'assets': [
                    _token_amount_to_data(amounts_in[0]),
                    _token_amount_to_data(amounts_in[1]),
                ],
                'slippage_tolerance': slippage_tolerance,
            }
        }
        coins = [
            amount.to_coin()
            for amount in amounts_in
            if isinstance(amount.token, TerraNativeToken)
        ]
        msgs.append(
            MsgExecuteContract(
                sender=sender,
                contract=self.contract_addr,
                execute_msg=execute_msg,
                coins=coins
            )
        )
        return msgs

    def op_add_liquidity(
        self,
        sender: str,
        amounts_in: AmountTuple,
        slippage_tolerance: Decimal = DEFAULT_ADD_LIQUIDITY_SLIPPAGE_TOLERANCE,
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        amount_out = self.get_add_liquidity_amount_out(amounts_in, slippage_tolerance)
        msgs = self.build_add_liquity_msgs(sender, amounts_in, slippage_tolerance)
        return amount_out, msgs

    def op_add_single_side(
        self,
        sender: str,
        amounts_in: AmountTuple,
        slippage_tolerance: Decimal = DEFAULT_ADD_LIQUIDITY_SLIPPAGE_TOLERANCE,
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        raise NotImplementedError


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
