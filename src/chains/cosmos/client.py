from typing import Protocol

from terra_sdk.core import AccAddress


class CosmosClient(Protocol):
    address: AccAddress

    async def contract_query(self, contract_addr: AccAddress, query_msg: dict) -> dict:
        ...

    async def contract_info(self, address: AccAddress) -> dict:
        ...

    async def get_bank(
        self,
        denoms: list[str] = None,
        address: AccAddress = None,
    ) -> list:
        ...
