from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .async_client import OsmosisClient


class Api:
    def __init__(self, client: OsmosisClient):
        self.client = client

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(client={self.client})"
