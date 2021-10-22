from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .async_client import TerraClient


class Api:
    def __init__(self, client: "TerraClient"):
        self.client = client

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(client={self.client})"
