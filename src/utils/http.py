"""Drop-in replacement for part of httpx module with some extra retry features."""
import logging
import re
import time
from collections import Counter
from typing import Iterable

import httpx
from httpx._types import URLTypes

log = logging.getLogger(__name__)


DEFAULT_TIMEOUT = 5.0
DEFAULT_N_TRIES = 3
DEFAULT_BACKOFF_FACTOR = 0.5
DEFAULT_STATUS_FORCELIST = (500, 502, 503, 504)


class Client(httpx.Client):
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

    def get(
        self,
        url: URLTypes,
        n_tries: int = DEFAULT_N_TRIES,
        **kwargs,
    ) -> httpx.Response:
        """httpx GET with default retries"""
        return request("GET", url, n_tries=n_tries, httpx_client=self, **kwargs)

    def post(
        self,
        url: URLTypes,
        n_tries: int = DEFAULT_N_TRIES,
        **kwargs,
    ) -> httpx.Response:
        """httpx POST with default retries"""
        return request("POST", url, n_tries=n_tries, httpx_client=self, **kwargs)


_DEFAULT_CLIENT = Client()


def get(url: URLTypes, **kwargs) -> httpx.Response:
    """httpx GET with default retries"""
    return _DEFAULT_CLIENT.get(url, **kwargs)


def post(url: URLTypes, **kwargs) -> httpx.Response:
    """httpx POST with default retries"""
    return _DEFAULT_CLIENT.post(url, **kwargs)


def request(
    method: str,
    url: URLTypes,
    n_tries: int = DEFAULT_N_TRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    status_forcelist: Iterable[int] = DEFAULT_STATUS_FORCELIST,
    http2: bool = True,
    httpx_client: httpx.Client = None,
    **kwargs,
) -> httpx.Response:
    kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
    if httpx_client is not None:
        return _send_request(
            httpx_client,
            method,
            url,
            n_tries=n_tries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            **kwargs,
        )
    with httpx.Client(http2=http2, timeout=kwargs["timeout"]) as client:
        return _send_request(
            client,
            method,
            url,
            n_tries=n_tries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            **kwargs,
        )


def _send_request(
    client: httpx.Client,
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
            res = client.request(method, url, **kwargs)
            res.raise_for_status()
            return res
        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            if status_code not in status_forcelist:
                raise
            log.debug(
                f"Error on http {method}, url={str(e.request.url)}, "
                f"{status_code=}, response={e.response.text!r}",
                exc_info=True,
            )
        except httpx.RequestError as e:
            url = e.request.url
            log.debug(f"Error on http {method}, url={str(e.request.url)}", exc_info=True)
        except Exception as e:
            log.debug(f"Error on http {method}, {client.base_url=} ({e!r})", exc_info=True)
        time.sleep((1 + backoff_factor) ** i - 1)
    raise httpx.HTTPError(f"httpx {method} failed after {n_tries=}")


def get_host_ip() -> str:
    ip_getter_service_urls = [
        "http://icanhazip.com",
        "http://ifconfig.me",
        "http://api.ipify.org",
        "http://bot.whatismyipaddress.com",
        "http://ipinfo.io/ip",
        "http://ipecho.net/plain",
    ]
    ips: list[str] = []
    for url in ip_getter_service_urls:
        res = get(url, follow_redirects=True)
        ip = res.text.strip()
        if re.match(r"^(?:\d{1,3}\.){3}\d{1,3}$", ip):
            ips.append(ip)
    return Counter(ips).most_common(1)[0][0]
