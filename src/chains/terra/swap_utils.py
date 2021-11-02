from terra_sdk.core.wasm import MsgExecuteContract

from .token import TerraTokenAmount

Operation = tuple[TerraTokenAmount, list[MsgExecuteContract]]
