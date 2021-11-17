import asyncio
import json
import logging
from typing import AsyncIterable, TypedDict

import websockets.client
import websockets.exceptions

log = logging.getLogger(__name__)

MAX_TIME_WAIT_EVENTS = 15


class SubscriptionMsg(TypedDict):
    jsonrpc: str
    method: str
    id: int
    params: list[str]


async def _subscribe_rpc(
    client: websockets.client.WebSocketClientProtocol,
    subscription_msg: SubscriptionMsg,
):
    await client.send(json.dumps(subscription_msg))
    while True:
        response = json.loads(await client.recv())
        if response["id"] == subscription_msg["id"]:
            return


def _extract_height(data: dict) -> int:
    try:
        return int(data["result"]["data"]["value"]["header"]["height"])
    except Exception as e:
        raise Exception(
            f"Could not parse websocket NewBlockHeader subscription ({e!r}): {data}"
        )


async def loop_latest_height(rpc_websocket_uri: str) -> AsyncIterable[int]:
    subscription_msg: SubscriptionMsg = {
        "jsonrpc": "2.0",
        "method": "subscribe",
        "id": 0,
        "params": ["tm.event='NewBlockHeader'"],
    }
    while True:
        try:
            async with websockets.client.connect(rpc_websocket_uri) as client:
                await asyncio.wait_for(
                    _subscribe_rpc(client, subscription_msg), MAX_TIME_WAIT_EVENTS
                )
                while True:
                    task_get_header = asyncio.wait_for(client.recv(), MAX_TIME_WAIT_EVENTS)
                    response = json.loads(await task_get_header)
                    yield _extract_height(response)
        except websockets.exceptions.ConnectionClosed:
            log.debug(f"Websocket connection {rpc_websocket_uri} closed, reconnecting")
            subscription_msg["id"] += 1


async def wait_next_block_height(rpc_websocket_uri: str) -> int:
    subscription_msg: SubscriptionMsg = {
        "jsonrpc": "2.0",
        "method": "subscribe",
        "id": 0,
        "params": ["tm.event='NewBlockHeader'"],
    }
    async with websockets.client.connect(rpc_websocket_uri) as client:
        await _subscribe_rpc(client, subscription_msg)
        response = json.loads(await client.recv())
        return _extract_height(response)
