from __future__ import annotations

import asyncio
import functools
import json
import logging
import traceback
from typing import Awaitable, Callable

from quart import Quart, Response, request

from chains import OsmosisClient, TerraClient
from chains.cosmos.client.api_broadcaster import BroadcasterPayload, BroadcasterResponse
from startup import setup

app = Quart(__name__)
log = logging.getLogger(__name__)

GIT_COMMIT = open("git_commit").read().strip()

_clients: dict[str, OsmosisClient | TerraClient] = {
    "osmosis": OsmosisClient(use_broadcaster=False, raise_on_syncing=False),
    "terra": TerraClient(use_broadcaster=False, raise_on_syncing=False),
}


async def _get_client(name: str) -> OsmosisClient | TerraClient:
    client = _clients[name]
    if not client.started:
        await client.start()
    return client


@app.before_serving
async def startup():
    setup()
    log.info(f"Running on git commit {GIT_COMMIT}")
    values = await asyncio.gather(
        *(client.start() for client in _clients.values()), return_exceptions=True
    )
    for val, client in zip(values, _clients.values()):
        if isinstance(val, Exception):
            log.exception(f"Error when starting {client}: {val!r}")


@app.after_serving
async def shutdown():
    await asyncio.gather(*(client.close() for client in _clients.values() if client.started))


def catch_unexpected_error(f: Callable[..., Awaitable]) -> Callable[..., Awaitable]:
    @functools.wraps(f)
    async def wrapper(*args, **kwargs):
        try:
            return await f(*args, **kwargs)
        except Exception as e:
            log.exception(e)
            response = {"message": "Unexpected error", "traceback": traceback.format_exc()}
            return Response(json.dumps(response), status=500, content_type="application/json")

    return wrapper


@app.route("/<string:chain>/lcd/<path:path>")
@catch_unexpected_error
async def lcd_get(chain: str, path: str):
    client = await _get_client(chain)
    res = await client.lcd_http_client.request(request.method, path)
    return await res.aread()


@app.route("/<string:chain>/txs", methods=["POST"])
@catch_unexpected_error
async def post_tx(chain: str):
    remote_addr = request.headers.get("remote-addr", "")
    client = await _get_client(chain)
    data: BroadcasterPayload = await request.get_json()
    log.debug(f"({remote_addr=}) Received BroadcasterPayload", extra={"data": data})
    if not data:
        log.warning(
            f"({remote_addr=}) Unable to parse data",
            extra={"data": await request.get_data()},
        )
        return Response(
            json.dumps({"message": "Unable to parse data"}),
            status=400,
            content_type="application/json",
        )
    res: BroadcasterResponse = await client.broadcaster.broadcast(data)
    if res["result"] == "repeated_tx":
        log.debug(f"({remote_addr=}) Repeated transaction")
    else:
        log.info(f"({remote_addr=}) Broadcasted transactions", extra={"data": res})
    return Response(json.dumps(res), content_type="application/json")


if __name__ == "__main__":
    app.run(debug=False)
