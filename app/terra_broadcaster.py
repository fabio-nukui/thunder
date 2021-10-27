from __future__ import annotations

import logging

from quart import Quart, request

from chains.terra import TerraClient
from startup import setup

app = Quart(__name__)
log = logging.getLogger(__name__)
client = TerraClient()


@app.before_serving
async def startup():
    setup()
    await client.start()


@app.after_serving
async def shutdown():
    await client.close()


@app.route("/lcd/<path:path>")
async def lcd_get(path):
    res = await client.lcd_http_client.request(request.method, path)
    return await res.aread()


@app.route("/tx", methods=["POST"])
async def post_tx():
    data = await request.get_data()
    log.info(data)
    return data


if __name__ == "__main__":
    app.run(debug=False)
