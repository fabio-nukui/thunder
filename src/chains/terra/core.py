from terra_sdk.core import Dec

from .client import TerraClient


class Token:
    def __init__(self, chain_id: str, contract_addr: str, symbol: str, decimals: int):
        self.chain_id = chain_id
        self.contract_addr = contract_addr
        self.symbol = symbol
        self.decimals = decimals

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.symbol})'

    def to_msg(self) -> dict:
        return {
            'token': {
                'contract_addr': self.contract_addr
            }
        }

    @classmethod
    def from_client(cls, client: TerraClient) -> Token:
        return cls()

