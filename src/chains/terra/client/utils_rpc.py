import json
import logging
from typing import AsyncIterable, TypedDict

import websockets.client
import websockets.exceptions

log = logging.getLogger(__name__)


class SubscriptionMsg(TypedDict):
    jsonrpc: str
    method: str
    id: int
    params: list[str]


async def _get_jsonrpc_subscription_ack(
    client: websockets.client.WebSocketClientProtocol,
    subscription_id: int,
):
    while True:
        response = json.loads(await client.recv())
        if response["id"] == subscription_id:
            return


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
                await client.send(json.dumps(subscription_msg))
                await _get_jsonrpc_subscription_ack(client, subscription_msg["id"])
                while True:
                    response = json.loads(await client.recv())
                    yield int(response["result"]["data"]["value"]["header"]["height"])
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
        await client.send(json.dumps(subscription_msg))
        await _get_jsonrpc_subscription_ack(client, subscription_msg["id"])
        response = json.loads(await client.recv())
        return int(response["result"]["data"]["value"]["header"]["height"])
