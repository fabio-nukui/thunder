from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from cosmos_sdk.core import AccAddress, Coin
from cosmos_sdk.core.ibc import MsgTransfer

from chains.cosmos.token import CosmosNativeToken, CosmosTokenAmount
from utils.cache import ttl_cache

from .base_api import Api

if TYPE_CHECKING:
    from .async_client import CosmosClient  # noqa: F401

_CHANNELS_DATA_TTL = 3600
DEFAULT_TIMEOUT_SECONDS = 60


class IbcApi(Api["CosmosClient"]):
    async def get_client_state(self, client_id: str) -> dict:
        url = f"ibc/core/client/v1/client_states/{client_id}"
        return (await self.client.lcd_http_client.get(url)).json()["client_state"]

    async def get_connection_data(self, connection_id: str) -> dict:
        url = f"ibc/core/connection/v1/connections/{connection_id}"
        connection = (await self.client.lcd_http_client.get(url)).json()["connection"]
        client = await self.get_client_state(connection["client_id"])
        return {
            "connection": connection,
            "client": client,
        }

    @ttl_cache(ttl=_CHANNELS_DATA_TTL)
    async def get_channels_data(self) -> list[dict]:
        response: list[dict] = []
        params: dict = {}
        while True:
            res = await self.client.lcd_http_client.get(
                "ibc/core/channel/v1/channels", params=params
            )
            data = res.json()
            channels_data = [c for c in data["channels"] if c["state"] == "STATE_OPEN"]
            connections_data = await asyncio.gather(
                *(self.get_connection_data(*d["connection_hops"]) for d in channels_data)
            )
            response.extend(
                [
                    {
                        **channel,
                        "connection_id": conn_data["connection"]["client_id"],
                        "counterparty_chain_id": conn_data["client"]["chain_id"],
                    }
                    for conn_data, channel in zip(connections_data, channels_data)
                    if conn_data["connection"]["state"] == "STATE_OPEN"
                ]
            )
            if not (pagination_key := data["pagination"]["next_key"]):
                return response
            params["pagination.key"] = pagination_key

    async def get_channels_by_chain(self, chain_id: str) -> list[str]:
        channels_data = await self.get_channels_data()
        response: list[str] = []
        for d in channels_data:
            if d["counterparty_chain_id"] == chain_id:
                response.append(d["channel_id"])
        return response

    async def get_chain_by_channel(self, channel_id: str) -> dict[str, str]:
        channels_data = await self.get_channels_data()
        for d in channels_data:
            if d["channel_id"] == channel_id:
                return d["counterparty_chain_id"]
        raise Exception(f"{channel_id=} not found")

    async def get_msg_transfer(
        self,
        receiver: str,
        amount: CosmosTokenAmount,
        chain_id: str = None,
        channel_id: str = None,
        sender: AccAddress = None,
        timeout: int = None,
    ) -> MsgTransfer:
        assert isinstance(amount.token, CosmosNativeToken)
        if channel_id is None:
            if chain_id is None:
                raise ValueError("One of channel_id or chain_id must be given")
            channel_ids = await self.get_channels_by_chain(chain_id)
            if len(channel_ids) > 1:
                raise Exception(f"Found multiple {channel_ids=}")
            channel_id = channel_ids[0]
        sender = sender or self.client.address
        timeout = DEFAULT_TIMEOUT_SECONDS if timeout is None else timeout
        return MsgTransfer(
            source_channel=channel_id,
            token=Coin(amount.token.denom, amount.int_amount),
            sender=sender,
            receiver=receiver,
            timeout_timestamp=time.time_ns() + timeout * 10 ** 9,
        )
