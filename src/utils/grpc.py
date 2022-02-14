from __future__ import annotations

import asyncio
from typing import cast

from grpclib.client import Channel, Stream
from grpclib.const import Cardinality
from grpclib.metadata import Deadline, _Metadata, _MetadataLike
from grpclib.stream import _RecvType, _SendType
from multidict import MultiDict

_DEFAULT_MAX_REQUESTS = 1000


class SemaphoreChannel(Channel):
    def __init__(self, *args, max_concurrent_requests: int = _DEFAULT_MAX_REQUESTS, **kwargs):
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        super().__init__(*args, **kwargs)

    def request(
        self,
        name: str,
        cardinality: Cardinality,
        request_type: type[_SendType],
        reply_type: type[_RecvType],
        *,
        timeout: float = None,
        deadline: Deadline = None,
        metadata: _MetadataLike = None,
    ) -> Stream[_SendType, _RecvType]:
        if timeout is not None and deadline is None:
            deadline = Deadline.from_timeout(timeout)
        elif timeout is not None and deadline is not None:
            deadline = min(Deadline.from_timeout(timeout), deadline)

        return SemaphoreStream(
            self,
            name,
            cast(_Metadata, MultiDict(metadata or ())),
            cardinality,
            request_type,
            reply_type,
            codec=self._codec,
            status_details_codec=self._status_details_codec,
            dispatch=self.__dispatch__,
            deadline=deadline,
        )


class SemaphoreStream(Stream):
    _channel: SemaphoreChannel

    async def send_message(self, message, *, end: bool = False) -> None:
        async with self._channel.semaphore:
            return await super().send_message(message, end=end)
