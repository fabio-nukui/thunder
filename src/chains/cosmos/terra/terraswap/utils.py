from typing import Sequence

from cosmos_sdk.core.msg import Msg

from ..token import TerraNativeToken, TerraToken, TerraTokenAmount

Operation = tuple[TerraTokenAmount, Sequence[Msg]]


def token_to_data(token: TerraToken) -> dict[str, dict[str, str]]:
    if isinstance(token, TerraNativeToken):
        return {"native_token": {"denom": token.denom}}
    return {"token": {"contract_addr": token.contract_addr}}
