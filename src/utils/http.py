"""Drop-in replacement for part of httpx module with some extra retry features."""
import logging
import time
from typing import Iterable

import httpx
from httpx._types import TimeoutTypes

log = logging.getLogger(__name__)


DEFAULT_N_TRIES = 3
DEFAULT_BACKOFF_FACTOR = 0.5
DEFAULT_STATUS_FORCELIST = (500, 502, 503, 504)


class Client(httpx.Client):
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

    def get(
        self,
        url: str,
        *args,
        timeout: TimeoutTypes = 5.0,
        n_tries: int = DEFAULT_N_TRIES,
        **kwargs,
    ) -> httpx.Response:
        """httpx GET with default retries"""
        return request(
            "GET", url, *args, timeout=timeout, n_tries=n_tries, httpx_client=self, **kwargs
        )

    def post(
        self,
        url: str,
        *args,
        timeout: TimeoutTypes = 5.0,
        n_tries: int = DEFAULT_N_TRIES,
        **kwargs,
    ) -> httpx.Response:
        """httpx POST with default retries"""
        return request(
            "POST", url, *args, timeout=timeout, n_tries=n_tries, httpx_client=self, **kwargs
        )


_DEFAULT_CLIENT = Client()


def get(url: str, *args, **kwargs) -> httpx.Response:
    """httpx GET with default retries"""
    return _DEFAULT_CLIENT.get(url, *args, **kwargs)


def post(url: str, *args, **kwargs) -> httpx.Response:
    """httpx POST with default retries"""
    return _DEFAULT_CLIENT.post(url, *args, **kwargs)


def request(
    method: str,
    url: str,
    *args,
    timeout: TimeoutTypes = 5.0,
    n_tries: int = DEFAULT_N_TRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    status_forcelist: Iterable[int] = DEFAULT_STATUS_FORCELIST,
    http2: bool = True,
    httpx_client: httpx.Client = None,
    **kwargs,
) -> httpx.Response:
    if httpx_client is not None:
        return _send_request(
            httpx_client,
            method,
            url,
            *args,
            n_tries=n_tries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            **kwargs,
        )
    with httpx.Client(http2=http2, timeout=timeout) as client:
        return _send_request(
            client,
            method,
            url,
            *args,
            n_tries=n_tries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            **kwargs,
        )


def _send_request(
    client: httpx.Client,
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
            res = client.request(method, url, *args, **kwargs)
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
        time.sleep((1 + backoff_factor) ** i - 1)
    raise httpx.HTTPError(f"httpx {method} failed after {n_tries=}")
