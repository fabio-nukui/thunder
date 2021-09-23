import binance


class BinanceClient:
    def __init__(self, api_key: str, api_secret: str):
        self.client = binance.Client(api_key, api_secret)
        self.dcm = binance.ThreadedDepthCacheManager(api_key, api_secret)
        self.dcm.start()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}'
