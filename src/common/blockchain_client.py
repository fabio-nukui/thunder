from __future__ import annotations

import logging
from abc import ABC, abstractclassmethod
from typing import Literal

from exceptions import NodeSyncing

log = logging.getLogger(__name__)


class BlockchainClient(ABC):
    height: int | Literal["latest"]

    def __init__(self, raise_on_syncing: bool = False) -> None:
        if raise_on_syncing and self.syncing:
            assert isinstance(self.height, int), f"Unexpected height={self.height}, expected int"
            raise NodeSyncing(self.height)

        log.info(f"Initialized {self} at height={self.height}")

    @property
    @abstractclassmethod
    def syncing(self) -> bool:
        ...
