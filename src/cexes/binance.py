from __future__ import annotations

import atexit
from decimal import Decimal

import binance
from binance.depthcache import DepthCache

from exceptions import InsufficientLiquidity


class TokenAmount:
    def __init__(self, symbol: str, amount: int | float | Decimal, precision: int = 8):
        self.symbol = symbol
        if isinstance(amount, float):
            amount = Decimal(str(amount))
        elif isinstance(amount, int):
            amount = Decimal(amount)
        self.amount = round(amount, precision)
        self.precision = precision

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.symbol}: {self.amount})'


class TradingPair:
    def __init__(self, symbol: str, client: binance.Client) -> None:
        self.symbol = symbol

        info = client.get_symbol_info(symbol)
        if info is None:
            raise Exception(f'Binance pair {symbol} not found')
        self.base_asset: str = info['baseAsset']
        self.quote_asset: str = info['quoteAsset']
        self.tick_size = self._get_price_tick_size(info['filters'])
        self.step_size = self._get_lot_step_size(info['filters'])

        self.bids: list[tuple[Decimal, Decimal]] = []
        self.asks: list[tuple[Decimal, Decimal]] = []

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.symbol})'

    @staticmethod
    def _get_price_tick_size(list_filters: dict) -> Decimal:
        for filter_ in list_filters:
            if filter_['filterType'] == 'PRICE_FILTER':
                return Decimal(filter_['tickSize'])
        raise Exception('PRICE_FILTER not found')

    @staticmethod
    def _get_lot_step_size(list_filters: dict) -> Decimal:
        for filter_ in list_filters:
            if filter_['filterType'] == 'LOT_SIZE':
                return Decimal(filter_['stepSize'])
        raise Exception('LOT_SIZE not found')

    def update_from_depth_cache(self, depth_cache: DepthCache):
        self.bids = depth_cache.get_bids()
        self.asks = depth_cache.get_asks()

    def get_market_quote(self, amount_in: TokenAmount) -> TokenAmount:
        symbol_out = self.base_asset if amount_in.symbol == self.quote_asset else self.quote_asset

        amount_out = Decimal(1)
        amount_left_to_trade = amount_in.amount
        if amount_in.symbol == self.base_asset:  # SELL order
            for price, qty in self.bids:
                quote_amount = price * qty
                if amount_left_to_trade <= qty:
                    return TokenAmount(symbol_out, amount_out + amount_left_to_trade * price)
                amount_left_to_trade -= qty
                amount_out += quote_amount
            raise InsufficientLiquidity
        elif amount_in.symbol == self.quote_asset:  # BUY order
            for price, qty in self.asks:
                quote_amount = price * qty
                if amount_left_to_trade <= quote_amount:
                    return TokenAmount(symbol_out, amount_out + amount_left_to_trade / price)
                amount_left_to_trade -= quote_amount
                amount_out += qty
            raise InsufficientLiquidity
        else:
            raise TypeError('amount_in not in pair')


class BinanceClient:
    def __init__(self, api_key: str, api_secret: str):
        self.client = binance.Client(api_key, api_secret)
        self.dcm = binance.ThreadedDepthCacheManager(api_key, api_secret)
        self._pairs: dict[str, TradingPair] = {}

        self.dcm.start()
        atexit.register(self.dcm.stop)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}'

    def get_trading_pair(self, symbol: str) -> TradingPair:
        if symbol not in self._pairs:
            pair = TradingPair(symbol, self.client)
            self.dcm.start_depth_cache(pair.update_from_depth_cache, symbol, conv_type=Decimal)
            self._pairs[symbol] = pair

        return self._pairs[symbol]
