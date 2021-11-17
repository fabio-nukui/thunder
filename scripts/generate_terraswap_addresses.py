#! /usr/bin/env python

import asyncio
import json
import logging
import os

from chains.cosmos.terra import TerraClient, terraswap
from startup import setup

log = logging.getLogger(__name__)


_TERRASWAP_ADDRESSES_DIR = "resources/addresses/terra/{chain_id}/terraswap"
_TERRASWAP_DEX_ROUTER = "terra19qx5xe6q9ll4w0890ux7lv2p4mf3csd4qvt3ex"
_TERRASWAP_DEX_ASSERT_LIMIT_ORDER = "terra1vs9jr7pxuqwct3j29lez3pfetuu8xmq7tk3lzk"


def get_filepath(chain_id: str, dex_name: str) -> str:
    return os.path.join(_TERRASWAP_ADDRESSES_DIR.format(chain_id=chain_id), f"{dex_name}.json")


async def write_to_file(
    factory: terraswap.Factory,
    name: str,
    router_address: str = None,
    assert_limit_order_address: str = None,
):
    log.info(f"Generating addresses for {name}")
    addresses = await factory.generate_addresses_dict(
        router_address=router_address, assert_limit_order_address=assert_limit_order_address
    )
    log.info(f"Created addresses for {name} with {len(addresses['pairs'])} pairs")

    filepath = get_filepath(factory.client.chain_id, name)
    with open(filepath, "w") as f:
        json.dump(addresses, f, indent=2)
        f.write("\n")

    log.info(f"Wrote addresses to {filepath}")


async def main():
    async with TerraClient() as client:
        terraswap_factory = await terraswap.TerraswapFactory.new(client)
        await write_to_file(
            terraswap_factory,
            "terraswap",
            _TERRASWAP_DEX_ROUTER,
            _TERRASWAP_DEX_ASSERT_LIMIT_ORDER,
        )

        loop_factory = await terraswap.LoopFactory.new(client)
        await write_to_file(loop_factory, "loop")


if __name__ == "__main__":
    setup()
    asyncio.run(main())
