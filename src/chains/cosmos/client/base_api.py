from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from .async_client import CosmosClient


CosmosClientT = TypeVar("CosmosClientT", bound="CosmosClient")


class Api(Generic[CosmosClientT]):
    def __init__(self, client: CosmosClientT):
        self.client = client

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(client={self.client})"
