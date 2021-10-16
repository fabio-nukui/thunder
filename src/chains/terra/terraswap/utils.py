import asyncio

from terra_sdk.core import AccAddress
from terra_sdk.core.wasm import MsgExecuteContract

from ..client import TerraClient
from ..token import CW20Token, TerraNativeToken, TerraToken, TerraTokenAmount

Operation = tuple[TerraTokenAmount, list[MsgExecuteContract]]


def token_to_data(token: TerraToken) -> dict[str, dict[str, str]]:
    if isinstance(token, TerraNativeToken):
        return {"native_token": {"denom": token.denom}}
    return {"token": {"contract_addr": token.contract_addr}}


async def pair_tokens_from_data(
    asset_infos: list[dict],
    client: TerraClient,
) -> tuple[TerraToken, TerraToken]:
    token_0, token_1 = await asyncio.gather(
        token_from_data(asset_infos[0], client),
        token_from_data(asset_infos[1], client),
    )
    return token_0, token_1


async def token_from_data(asset_info: dict, client: TerraClient) -> TerraToken:
    if "native_token" in asset_info:
        return TerraNativeToken(asset_info["native_token"]["denom"])
    if "token" in asset_info:
        contract_addr: AccAddress = asset_info["token"]["contract_addr"]
        return await CW20Token.from_contract(contract_addr, client)
    raise TypeError(f"Unexpected data format: {asset_info}")
