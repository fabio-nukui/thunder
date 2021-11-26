from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import TYPE_CHECKING

from .base_api import Api

if TYPE_CHECKING:
    from .async_client import CosmosClient  # noqa: F401


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

    async def get_channels_by_chain(self) -> dict[str, list[str]]:
        channels_data = await self.get_channels_data()
        response: dict[str, list[str]] = defaultdict(list)
        for d in channels_data:
            response[d["counterparty_chain_id"]].append(d["channel_id"])
        return dict(response)

    async def get_chain_by_channel(self) -> dict[str, str]:
        channels_data = await self.get_channels_data()
        return {d["channel_id"]: d["counterparty_chain_id"] for d in channels_data}
