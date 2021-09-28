from __future__ import annotations

import atexit
import time
from decimal import Decimal
from threading import Lock

import binance
from binance.depthcache import DepthCache, ThreadedDepthCacheManager

import auth_secrets
from common.token import Token, TokenAmount
from exceptions import InsufficientLiquidity

DEFAULT_PRECISION = 8
DEPTH_CACHE_LIMIT = 1000
DEPTH_CACHE_REFRESH_INTERVAL = 1800


class BinanceToken(Token):
    def __init__(self, symbol: str, decimals: int = DEFAULT_PRECISION) -> None:
        self.symbol = symbol
        self.decimals = decimals

    @property
    def _id(self) -> tuple:
        return (self.symbol, self.decimals)


class BinanceTokenAmount(TokenAmount):
    token: BinanceToken


class TradingPair:
    def __init__(
        self,
        symbol: str,
        client: binance.Client,
        dcm: binance.ThreadedDepthCacheManager,
    ) -> None:
        self.symbol = symbol

        self.bids: list[tuple[Decimal, Decimal]] = []
        self.asks: list[tuple[Decimal, Decimal]] = []

        info = client.get_symbol_info(symbol)
        if info is None:
            raise Exception(f'Binance pair {symbol} not found')
        self.base_asset = BinanceToken(info['baseAsset'], info['baseAssetPrecision'])
        self.quote_asset = BinanceToken(info['quoteAsset'], info['quoteAssetPrecision'])

        self.price_tick_size = self._get_filter_value(info['filters'], 'PRICE_FILTER', 'tickSize')
        self.lot_step_size = self._get_filter_value(info['filters'], 'LOT_SIZE', 'stepSize')
        self.lot_min_size = self._get_filter_value(info['filters'], 'LOT_SIZE', 'minQty')
        self.min_notional = self._get_filter_value(info['filters'], 'MIN_NOTIONAL', 'minNotional')

        self._ready = False
        self._ready_lock = Lock()
        self._start_depth_cache(dcm)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.symbol})'

    @staticmethod
    def _get_filter_value(list_filters: dict, filter_type: str, key: str) -> Decimal:
        for filter_ in list_filters:
            if filter_['filterType'] == filter_type:
                return Decimal(filter_[key])
        raise Exception(f'{filter_type} not found')

    def update_from_depth_cache(self, depth_cache: DepthCache):
        self.bids = depth_cache.get_bids()
        self.asks = depth_cache.get_asks()
        if not self._ready:
            with self._ready_lock:
                self._ready = True

    def get_market_amount_out(self, amount_in: BinanceTokenAmount) -> BinanceTokenAmount:
        amount_out = Decimal(0)
        amount_left_to_trade = amount_in.amount

        if amount_in.token == self.base_asset:  # SELL order
            for price, qty in self.bids:
                quote_amount = price * qty
                if amount_left_to_trade <= qty:
                    amount_out = amount_out + amount_left_to_trade * price
                    return BinanceTokenAmount(self.quote_asset, amount_out)
                amount_left_to_trade -= qty
                amount_out += quote_amount
            raise InsufficientLiquidity
        elif amount_in.token == self.quote_asset:  # BUY order
            for price, qty in self.asks:
                quote_amount = price * qty
                if amount_left_to_trade <= quote_amount:
                    amount_out = amount_out + amount_left_to_trade / price
                    return BinanceTokenAmount(self.base_asset, amount_out)
                amount_left_to_trade -= quote_amount
                amount_out += qty
            raise InsufficientLiquidity
        else:
            raise TypeError(f'{amount_in.token=} not in pair')

    def _start_depth_cache(self, dcm: ThreadedDepthCacheManager):
        dcm.start_depth_cache(
            self.update_from_depth_cache,
            self.symbol,
            refresh_interval=DEPTH_CACHE_REFRESH_INTERVAL,
            limit=DEPTH_CACHE_LIMIT,
            conv_type=Decimal,
        )
        for _ in range(100):
            with self._ready_lock:
                if self._ready:
                    return
            time.sleep(0.1)
        raise Exception('Timeout on DepthCacheManager initialization')


class BinanceClient:
    def __init__(self, api_key: str = None, api_secret: str = None):
        if api_key is None or api_secret is None:
            binance_secret = auth_secrets.binance_api()
            api_key = binance_secret['api_key']
            api_secret = binance_secret['api_secret']

        self.client = binance.Client(api_key, api_secret)
        self.dcm = ThreadedDepthCacheManager(api_key, api_secret)
        self._pairs: dict[str, TradingPair] = {}

        self.dcm.start()
        atexit.register(self.dcm.stop)  # type: ignore
        self._wait_dcm_ready()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}'

    def get_trading_pair(self, symbol: str) -> TradingPair:
        if symbol not in self._pairs:
            self._pairs[symbol] = TradingPair(symbol, self.client, self.dcm)

        return self._pairs[symbol]

    def _wait_dcm_ready(self):
        for _ in range(2000):
            if self.dcm._client is not None:
                return
            time.sleep(0.001)
        raise Exception('Timeout on ThreadedDepthCacheManager initialization')
