"""Drop-in replacement for part of httpx module with some extra retry features. (async version)"""
import logging
import time
from typing import Iterable

import httpx
from httpx._types import TimeoutTypes

log = logging.getLogger(__name__)


DEFAULT_N_TRIES = 4
DEFAULT_BACKOFF_FACTOR = 0.5
DEFAULT_STATUS_FORCELIST = (500, 502, 503, 504)


class AsyncClient(httpx.AsyncClient):
    def __init__(
        self,
        *args,
        n_tries: int = DEFAULT_N_TRIES,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
        status_forcelist: Iterable[int] = DEFAULT_STATUS_FORCELIST,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.n_tries = n_tries
        self.backoff_factor = backoff_factor
        self.status_forcelist = status_forcelist

    async def get(self, *args, **kwargs):
        return await get(
            *args,
            n_tries=self.n_tries,
            backoff_factor=self.backoff_factor,
            status_forcelist=self.status_forcelist,
            httpx_client=self,
            **kwargs,
        )

    async def post(self, *args, **kwargs):
        return await post(
            *args,
            n_tries=self.n_tries,
            backoff_factor=self.backoff_factor,
            status_forcelist=self.status_forcelist,
            httpx_client=self,
            **kwargs,
        )


async def get(
    url: str,
    *args,
    timeout: TimeoutTypes = 5.0,
    n_tries: int = DEFAULT_N_TRIES,
    httpx_client: httpx.AsyncClient = None,
    **kwargs,
) -> httpx.Response:
    """httpx GET with default retries"""
    return await request(
        "GET", url, *args, timeout=timeout, n_tries=n_tries, httpx_client=httpx_client, **kwargs
    )


async def post(
    url: str,
    *args,
    timeout: TimeoutTypes = 5.0,
    n_tries: int = DEFAULT_N_TRIES,
    httpx_client: httpx.AsyncClient = None,
    **kwargs,
) -> httpx.Response:
    """httpx POST with default retries"""
    return await request(
        "POST", url, *args, timeout=timeout, n_tries=n_tries, httpx_client=httpx_client, **kwargs
    )


async def request(
    method: str,
    url: str,
    *args,
    timeout: TimeoutTypes = 5.0,
    n_tries: int = DEFAULT_N_TRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    status_forcelist: Iterable[int] = DEFAULT_STATUS_FORCELIST,
    http2: bool = True,
    httpx_client: httpx.AsyncClient = None,
    **kwargs,
) -> httpx.Response:
    if httpx_client is not None:
        return await _send_request(
            httpx_client,
            method,
            url,
            *args,
            n_tries=n_tries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            **kwargs,
        )
    async with httpx.AsyncClient(http2=http2, timeout=timeout) as client:
        return await _send_request(
            client,
            method,
            url,
            *args,
            n_tries=n_tries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            **kwargs,
        )


async def _send_request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *args,
    n_tries: int,
    backoff_factor: float,
    status_forcelist: Iterable[int],
    **kwargs,
) -> httpx.Response:
    """httpx request with default retries.
    inspired by https://www.peterbe.com/plog/best-practice-with-retries-with-requests"""
    for i in range(n_tries):
        try:
            res = await client.request(method, url, *args, **kwargs)
            res.raise_for_status()
            return res
        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            if status_code not in status_forcelist:
                raise
            log.debug(f"Error on http {method}, {status_code=}", exc_info=True)
        except Exception as e:
            log.debug(f"Error on http {method} ({e})", exc_info=True)
        time.sleep((1 + backoff_factor) ** i - 1)
    raise httpx.HTTPError(f"httpx {method} failed after {n_tries=}")
