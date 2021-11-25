from __future__ import annotations

import asyncio
import json

import yaml

DOCKER_CMD = ["docker", "exec", "osmosis_node"]
RESULT_FILEPATH = "resources/contracts/cosmos/osmosis-1/ibc_tokens.json"


async def get_cmd_stdout(cmd: str | bytes) -> bytes:
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    if stderr:
        raise Exception(stderr.decode())
    return stdout


async def get_bank_total(docker: bool = True) -> list[dict[str, str]]:
    cmd = ["osmosisd", "query", "bank", "total"]
    if docker:
        cmd = DOCKER_CMD + cmd
    res = await get_cmd_stdout(" ".join(cmd))
    return yaml.safe_load(res)["supply"]


async def get_denom_trace(denom_hash: str, docker: bool = True) -> dict:
    cmd = ["osmosisd", "query", "ibc-transfer", "denom-trace", denom_hash]
    if docker:
        cmd = DOCKER_CMD + cmd
    res = await get_cmd_stdout(" ".join(cmd))
    return {"denom_hash": denom_hash, **yaml.safe_load(res)["denom_trace"]}


def main():
    loop = asyncio.get_event_loop()
    supplies = loop.run_until_complete(get_bank_total())
    tasks = (get_denom_trace(s["denom"][4:]) for s in supplies if s["denom"].startswith("ibc/"))
    data = loop.run_until_complete(asyncio.gather(*tasks))
    data = sorted(data, key=lambda x: x["denom_hash"])
    with open(RESULT_FILEPATH, "w") as f:
        json.dump(data, f, indent=2)


if __name__ == "__main__":
    main()
