from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from terra_sdk.core.tx import Tx

log = logging.getLogger(__name__)


class Filter(ABC):
    @abstractmethod
    def match_tx(self, tx: Tx) -> bool:
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}"

    def __and__(self: Filter, other) -> FilterAll:
        if not isinstance(other, Filter):
            return NotImplemented
        self_filters = self.filters if isinstance(self, FilterAll) else [self]
        other_filters = other.filters if isinstance(other, FilterAll) else [other]
        return FilterAll(self_filters + other_filters)

    def __or__(self: Filter, other) -> FilterAny:
        if not isinstance(other, Filter):
            return NotImplemented
        self_filters = self.filters if isinstance(self, FilterAny) else [self]
        other_filters = other.filters if isinstance(other, FilterAny) else [other]
        return FilterAny(self_filters + other_filters)


class FilterAll(Filter):
    def __init__(self, filters: list[Filter]):
        self.filters = filters

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.filters})"

    def match_tx(self, tx: Tx) -> bool:
        return all(filter_.match_tx(tx) for filter_ in self.filters)


class FilterAny(Filter):
    def __init__(self, filters: list[Filter]):
        self.filters = filters

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.filters})"

    def match_tx(self, tx: Tx) -> bool:
        return any(filter_.match_tx(tx) for filter_ in self.filters)


class FilterMsgsLength(Filter):
    def __init__(self, length: int):
        self.length = length

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(length={self.length})"

    def match_tx(self, tx: Tx) -> bool:
        return len(tx.body.messages) == self.length
