from __future__ import annotations

from enum import Enum, auto
from typing import Sequence

from cosmos_sdk.core.msg import Msg

from ..token import TerraNativeToken, TerraToken, TerraTokenAmount

Operation = tuple[TerraTokenAmount, Sequence[Msg]]


class EncodingVersion(Enum):
    v1 = auto()
    v2 = auto()


def token_to_data(
    token: TerraToken,
    version: EncodingVersion,
) -> dict[str, dict[str, str]] | dict[str, str]:
    if version == EncodingVersion.v1:
        if isinstance(token, TerraNativeToken):
            return {"native_token": {"denom": token.denom}}
        return {"token": {"contract_addr": token.contract_addr}}
    if version == EncodingVersion.v2:
        if isinstance(token, TerraNativeToken):
            return {"native": token.denom}
        return {"cw20": token.contract_addr}
    raise Exception("Should never reach")
