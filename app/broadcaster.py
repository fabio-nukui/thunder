from __future__ import annotations

import asyncio
import json
import logging
import traceback

from quart import Quart, Response, request

from chains import OsmosisClient, TerraClient
from chains.cosmos.client.api_broadcaster import BroadcasterPayload, BroadcasterResponse
from startup import setup

app = Quart(__name__)
log = logging.getLogger(__name__)

clients: dict[str, OsmosisClient | TerraClient] = {
    "osmosis": OsmosisClient(use_broadcaster=False, raise_on_syncing=False),
    "terra": TerraClient(use_broadcaster=False, raise_on_syncing=False),
}

GIT_COMMIT = open("git_commit").read().strip()


@app.before_serving
async def startup():
    setup()
    log.info(f"Running on git commit {GIT_COMMIT}")
    await asyncio.gather(
        *(client.start() for client in clients.values()), return_exceptions=True
    )


@app.after_serving
async def shutdown():
    await asyncio.gather(
        *(client.close() for client in clients.values()), return_exceptions=True
    )


@app.route("/<string:chain>/lcd/<path:path>")
async def lcd_get(chain: str, path: str):
    try:
        res = await clients[chain].lcd_http_client.request(request.method, path)
        return await res.aread()
    except Exception as e:
        msg = f"Error when querying local LCD endpoint {e!r}"
        log.debug(msg, exc_info=True)
        return Response(
            json.dumps({"message": msg}), status=500, content_type="application/json"
        )


@app.route("/<string:chain>/txs", methods=["POST"])
async def post_tx(chain: str):
    remote_addr = request.headers.get("remote-addr", "")
    try:
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
        res: BroadcasterResponse = await clients[chain].broadcaster.broadcast(data)
        if res["result"] == "repeated_tx":
            log.debug(f"({remote_addr=}) Repeated transaction")
        else:
            log.info(f"({remote_addr=}) Broadcasted transactions", extra={"data": res})
        return Response(json.dumps(res), content_type="application/json")
    except Exception as e:
        log.exception(e)
        response = {"message": "Unexpected error", "traceback": traceback.format_exc()}
        return Response(json.dumps(response), status=500, content_type="application/json")


if __name__ == "__main__":
    app.run(debug=False)