from terra_sdk.core.wasm import MsgExecuteContract

from ..token import TerraNativeToken, TerraToken, TerraTokenAmount

Operation = tuple[TerraTokenAmount, list[MsgExecuteContract]]


def token_to_data(token: TerraToken) -> dict[str, dict[str, str]]:
    if isinstance(token, TerraNativeToken):
        return {"native_token": {"denom": token.denom}}
    return {"token": {"contract_addr": token.contract_addr}}
