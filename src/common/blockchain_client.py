from __future__ import annotations

import logging
from abc import ABC, abstractclassmethod
from typing import Literal

from exceptions import NodeSyncing

log = logging.getLogger(__name__)


class BlockchainClient(ABC):
    block: int | Literal["latest"]

    def __init__(self, raise_on_syncing: bool = False) -> None:
        if raise_on_syncing and self.syncing:
            assert isinstance(self.block, int), f"Unexpected block={self.block}, expected int"
            raise NodeSyncing(self.block)

        log.info(f"Initialized {self} at block={self.block}")

    @property
    @abstractclassmethod
    def syncing(self) -> bool:
        ...
