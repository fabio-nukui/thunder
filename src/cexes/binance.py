

class BinanceClient:
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}'
