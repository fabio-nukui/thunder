"""Drop-in replacement for part of httpx module with some extra retry features. (async version)"""
import asyncio
import logging
from typing import Iterable

import httpx
from httpx._types import URLTypes

log = logging.getLogger(__name__)


DEFAULT_TIMEOUT = 5.0
DEFAULT_N_TRIES = 3
DEFAULT_BACKOFF_FACTOR = 0.5
DEFAULT_STATUS_FORCELIST = (500, 502, 503, 504)


class AsyncClient(httpx.AsyncClient):
    def __init__(
        self,
        n_tries: int = DEFAULT_N_TRIES,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
        status_forcelist: Iterable[int] = DEFAULT_STATUS_FORCELIST,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.n_tries = n_tries
        self.backoff_factor = backoff_factor
        self.status_forcelist = status_forcelist

    async def get(
        self,
        url: URLTypes,
        n_tries: int = DEFAULT_N_TRIES,
        **kwargs,
    ) -> httpx.Response:
        """httpx GET with default retries"""
        return await request("GET", url, n_tries=n_tries, httpx_client=self, **kwargs)

    async def post(
        self,
        url: URLTypes,
        n_tries: int = DEFAULT_N_TRIES,
        **kwargs,
    ) -> httpx.Response:
        """httpx POST with default retries"""
        return await request("POST", url, n_tries=n_tries, httpx_client=self, **kwargs)


_DEFAULT_CLIENT = AsyncClient()


async def get(url: str, **kwargs) -> httpx.Response:
    """httpx GET with default retries"""
    return await _DEFAULT_CLIENT.get(url, **kwargs)


async def post(url: str, **kwargs) -> httpx.Response:
    """httpx POST with default retries"""
    return await _DEFAULT_CLIENT.post(url, **kwargs)


async def request(
    method: str,
    url: URLTypes,
    n_tries: int = DEFAULT_N_TRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    status_forcelist: Iterable[int] = DEFAULT_STATUS_FORCELIST,
    http2: bool = True,
    httpx_client: httpx.AsyncClient = None,
    **kwargs,
) -> httpx.Response:
    kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
    if httpx_client is not None:
        return await _send_request(
            httpx_client,
            method,
            url,
            n_tries=n_tries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            **kwargs,
        )
    async with httpx.AsyncClient(http2=http2, timeout=kwargs["timeout"]) as client:
        return await _send_request(
            client,
            method,
            url,
            n_tries=n_tries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            **kwargs,
        )


async def _send_request(
    client: httpx.AsyncClient,
    method: str,
    url: URLTypes,
    n_tries: int,
    backoff_factor: float,
    status_forcelist: Iterable[int],
    **kwargs,
) -> httpx.Response:
    """httpx request with default retries.
    inspired by https://www.peterbe.com/plog/best-practice-with-retries-with-requests"""
    for i in range(n_tries):
        try:
            res = await client.request(method, url, **kwargs)
            res.raise_for_status()
            return res
        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            url = e.request.url
            if status_code not in status_forcelist:
                raise
            log.debug(f"Error on http {method}, {url=}, {status_code=}", exc_info=True)
        except Exception as e:
            log.debug(f"Error on http {method} ({e})", exc_info=True)
        await asyncio.sleep((1 + backoff_factor) ** i - 1)
    raise httpx.HTTPError(f"httpx {method} failed after {n_tries=}")
