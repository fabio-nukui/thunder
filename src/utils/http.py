import logging
import time
from typing import Iterable

import httpx
from httpx._types import TimeoutTypes

log = logging.getLogger(__name__)


def request(
    method: str,
    url: str,
    *args,
    timeout: TimeoutTypes = 5.0,
    n_tries: int = 4,
    backoff_factor: float = 0.5,
    status_forcelist: Iterable[int] = (500, 502, 503, 504),
    http2: bool = True,
    **kwargs,
) -> httpx.Response:
    """httpx request with default retries.
    inspired by https://www.peterbe.com/plog/best-practice-with-retries-with-requests"""
    with httpx.Client(http2=http2, timeout=timeout) as client:
        for i in range(n_tries):
            try:
                res = client.request(method, url, *args, **kwargs)
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


def get(
    url: str,
    *args,
    timeout: TimeoutTypes = 5.0,
    n_tries: int = 4,
    **kwargs,
) -> httpx.Response:
    """httpx GET with default retries"""
    return request("GET", url, *args, timeout=timeout, n_tries=n_tries, **kwargs)


def post(
    url: str,
    *args,
    timeout: TimeoutTypes = 5.0,
    n_tries: int = 4,
    **kwargs,
) -> httpx.Response:
    """httpx POST with default retries"""
    return request("POST", url, *args, timeout=timeout, n_tries=n_tries, **kwargs)
