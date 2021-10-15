from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Literal, TypeVar

from exceptions import NodeSyncing

log = logging.getLogger(__name__)


class BlockchainClient(ABC):
    height: int | Literal["latest"]


class SyncBlockchainClient(BlockchainClient, ABC):
    def __init__(self, raise_on_syncing: bool = False) -> None:
        if raise_on_syncing and self.syncing:
            assert isinstance(self.height, int), f"Unexpected height={self.height}, expected int"
            raise NodeSyncing(self.height)

        log.info(f"Initialized {self} at height={self.height}")

    @property
    @abstractmethod
    def syncing(self) -> bool:
        ...


_AsyncBlockchainClientT = TypeVar("_AsyncBlockchainClientT", bound="AsyncBlockchainClient")


class AsyncBlockchainClient(BlockchainClient, ABC):
    @classmethod
    @abstractmethod
    async def new(cls: type[_AsyncBlockchainClientT], *args, **kwargs) -> _AsyncBlockchainClientT:
        ...

    async def init(self, raise_on_syncing: bool = False):
        if raise_on_syncing and await self.is_syncing():
            assert isinstance(self.height, int), f"Unexpected height={self.height}, expected int"
            raise NodeSyncing(self.height)

        log.info(f"Initialized {self} at height={self.height}")

    @abstractmethod
    async def is_syncing(self) -> bool:
        ...
