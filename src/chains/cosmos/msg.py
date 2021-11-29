from __future__ import annotations

from typing import Union

from betterproto import Message
from cosmos_sdk.core.msg import Msg as TerraMsg


class Msg(TerraMsg):
    @staticmethod
    def from_data(data: dict) -> Msg:
        from .utils import parse_msg

        return parse_msg(data)

    @staticmethod
    def from_proto(data: Message) -> Msg:
        from .utils import parse_proto

        return parse_proto(data)


MsgType = Union[Msg, TerraMsg]
