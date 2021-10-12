from __future__ import annotations

import json
from decimal import Decimal

from web3.exceptions import BadFunctionCallOutput, ContractLogicError
from web3.main import Web3

from utils.cache import CacheGroup, ttl_cache

from .client import EVMClient
from .core import ERC20Token, EVMNativeToken, EVMToken, EVMTokenAmount

LENDING_PRECISION: int = 10 ** 18
PRECISION: int = 10 ** 18
FEE_DENOMINATOR: int = 10 ** 10
N_ITERATIONS = 255  # Number of iterations for numeric calculations

N_POOLS_CACHE = 10  # Must be at least equal to number of pools in strategy
BASE_POOL_ABI: dict = json.load(open("resources/contracts/evm/abis/curve/BasePool.json"))
NATIVE_TOKEN_ADDRESS = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"


class CurvePool:
    def __init__(
        self,
        client: EVMClient,
        address: str,
        pool_abi: dict = BASE_POOL_ABI,
    ):
        self.client = client
        self.chain_id = self.client.chain_id
        self.address = Web3.toChecksumAddress(address)
        self.contract = client.w3.eth.contract(self.address, abi=pool_abi)

        self.tokens = self._get_tokens()
        self.n_coins = len(self.tokens)
        self._rates: tuple[int, ...] = tuple(10 ** t.decimals for t in self.tokens)

        self._raw_fee: int = self.contract.functions.fee().call(block_identifier=self.client.height)
        self.fee = Decimal(self._raw_fee) / FEE_DENOMINATOR
        self._reserves = tuple(EVMTokenAmount(token) for token in self.tokens)
        self._reserves = tuple(EVMTokenAmount(token) for token in self.tokens)

    def __repr__(self):
        return f'{self.__class__.__name__}({"-".join(t.symbol for t in self.tokens)})'

    @property
    def reserves(self) -> tuple[EVMTokenAmount, ...]:
        for reserve, balance in zip(self._reserves, self._get_balances()):
            reserve.int_amount = balance
        return self._reserves

    def get_amount_out(self, amount_in: EVMTokenAmount, token_out: EVMToken) -> EVMTokenAmount:
        amount_out = self._get_dy(
            self.tokens.index(amount_in.token),
            self.tokens.index(token_out),
            amount_in.int_amount,
        )
        return EVMTokenAmount(token_out, int_amount=amount_out)

    def _get_tokens(self) -> tuple[EVMToken, ...]:
        i = 0
        tokens: list[EVMToken] = []
        while True:
            try:
                coins_func = self.contract.functions.coins(i)
                token_address = coins_func.call(block_identifier=self.client.height)
            except (BadFunctionCallOutput, ContractLogicError):
                return tuple(tokens)
            if token_address == NATIVE_TOKEN_ADDRESS:
                tokens.append(EVMNativeToken(self.chain_id))
            else:
                tokens.append(ERC20Token(token_address, client=self.client))
            i += 1

    @ttl_cache(CacheGroup.ETHEREUM, N_POOLS_CACHE)
    def _get_balances(self) -> list[int]:
        return [
            self.contract.functions.balances(i).call(block_identifier=self.client.height)
            for i in range(self.n_coins)
        ]

    # Internal functions based from curve's 3pool contract:
    # https://github.com/curvefi/curve-contract/blob/master/contracts/pools/3pool/StableSwap3Pool.vy

    # _A should vary slowly over time, cache can have greater TTL
    @ttl_cache(CacheGroup.ETHEREUM, N_POOLS_CACHE, ttl=3600)
    def _A(self):
        return self.contract.functions.A().call(block_identifier=self.client.height)

    def _xp(self) -> tuple[int, ...]:
        return tuple(
            rate * balance // LENDING_PRECISION
            for rate, balance in zip(self._rates, self._get_balances())
        )

    def _get_D(self, xp: tuple[int, ...], amp: int) -> int:
        S = 0
        for _x in xp:
            S += _x
        if S == 0:
            return 0

        Dprev = 0
        D = S
        Ann = amp * self.n_coins
        for _i in range(N_ITERATIONS):
            D_P = D
            for _x in xp:
                # If division by 0, this will be borked: only withdrawal will work. And that is good
                D_P = D_P * D // (_x * self.n_coins)
            Dprev = D
            D = (Ann * S + D_P * self.n_coins) * D // ((Ann - 1) * D + (self.n_coins + 1) * D_P)
            if abs(D - Dprev) <= 1:
                break
        return D

    def _get_y(self, i: int, j: int, x: int, xp_: tuple[int, ...], amp: int) -> int:
        # x in the input is converted to the same price/precision

        assert i != j
        assert j >= 0
        assert j < self.n_coins

        # should be unreachable, but good for safety
        assert i >= 0
        assert i < self.n_coins

        D = self._get_D(xp_, amp)
        c = D
        S_ = 0
        Ann = amp * self.n_coins

        _x = 0
        for _i in range(self.n_coins):
            if _i == i:
                _x = x
            elif _i != j:
                _x = xp_[_i]
            else:
                continue
            S_ += _x
            c = c * D // (_x * self.n_coins)
        c = c * D // (Ann * self.n_coins)
        b = S_ + D // Ann  # - D
        y_prev = 0
        y = D
        for _i in range(N_ITERATIONS):
            y_prev = y
            y = (y * y + c) // (2 * y + b - D)
            if abs(y - y_prev) <= 1:
                break
        return y

    def _get_dy(self, i: int, j: int, dx: int) -> int:
        dx = int(dx)
        # Fetch all data from blockchain in beggining of call
        _xp = self._xp()
        amp = self._A()

        x = _xp[i] + (dx * self._rates[i] // PRECISION)
        y = self._get_y(i, j, x, _xp, amp)
        dy = (_xp[j] - y - 1) * PRECISION // self._rates[j]
        fee = self._raw_fee * dy // FEE_DENOMINATOR
        return dy - fee
