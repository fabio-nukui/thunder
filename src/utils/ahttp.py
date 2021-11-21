"""Drop-in replacement for part of httpx module with some extra retry features. (async)"""
import asyncio
import logging
import re
from collections import Counter
from typing import Iterable

import httpx
from httpx._types import URLTypes

log = logging.getLogger(__name__)


DEFAULT_TIMEOUT = 5.0
DEFAULT_N_TRIES = 3
DEFAULT_BACKOFF_FACTOR = 0.5
DEFAULT_STATUS_FORCELIST = (500, 502, 503, 504)
DEFAULT_MAX_CONCURRENT_REQUESTS = 50


class AsyncClient(httpx.AsyncClient):
    def __init__(
        self,
        n_tries: int = DEFAULT_N_TRIES,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
        status_forcelist: Iterable[int] = DEFAULT_STATUS_FORCELIST,
        max_concurrent_requests: int = DEFAULT_MAX_CONCURRENT_REQUESTS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.n_tries = n_tries
        self.backoff_factor = backoff_factor
        self.status_forcelist = status_forcelist
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(base_url={str(self.base_url)})"

    async def get(
        self,
        url: URLTypes,
        n_tries: int = DEFAULT_N_TRIES,
        supress_logs: bool = False,
        **kwargs,
    ) -> httpx.Response:
        """httpx GET with default retries"""
        async with self._semaphore:
            return await request(
                "GET",
                url,
                n_tries=n_tries,
                httpx_client=self,
                supress_logs=supress_logs,
                **kwargs,
            )

    async def post(
        self,
        url: URLTypes,
        n_tries: int = DEFAULT_N_TRIES,
        supress_logs: bool = False,
        **kwargs,
    ) -> httpx.Response:
        """httpx POST with default retries"""
        async with self._semaphore:
            return await request(
                "POST",
                url,
                n_tries=n_tries,
                httpx_client=self,
                supress_logs=supress_logs,
                **kwargs,
            )

    async def check_connection(self, check_url: URLTypes) -> bool:
        try:
            await self.get(check_url, n_tries=1)
        except Exception as e:
            log.warning(f"Connection failed ({e!r}): base_url={str(self.base_url)}")
            return False
        else:
            log.debug(f"Connection OK: base_url={str(self.base_url)}")
            return True


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
    httpx_client: AsyncClient = None,
    supress_logs: bool = False,
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
            supress_logs=supress_logs,
            **kwargs,
        )
    async with AsyncClient(http2=http2, timeout=kwargs["timeout"]) as client:
        return await _send_request(
            client,
            method,
            url,
            n_tries=n_tries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            supress_logs=supress_logs,
            **kwargs,
        )


async def _send_request(
    client: AsyncClient,
    method: str,
    url: URLTypes,
    n_tries: int,
    backoff_factor: float,
    status_forcelist: Iterable[int],
    supress_logs: bool,
    **kwargs,
) -> httpx.Response:
    """httpx request with default retries.
    inspired by https://www.peterbe.com/plog/best-practice-with-retries-with-requests"""
    errors: list[Exception] = []
    if n_tries == 1:
        res = await client.request(method, url, **kwargs)
        res.raise_for_status()
        return res
    for i in range(n_tries):
        try:
            res = await client.request(method, url, **kwargs)
            res.raise_for_status()
            return res
        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            if status_code not in status_forcelist:
                raise
            if not supress_logs:
                log.debug(
                    f"Error on http {method}, url={str(e.request.url)}, "
                    f"{status_code=}, response={e.response.text!r}",
                    exc_info=True,
                )
            errors.append(e)
        except httpx.RequestError as e:
            url = e.request.url
            if not supress_logs:
                log.debug(f"Error on http {method} url={str(e.request.url)}", exc_info=True)
            errors.append(e)
        except Exception as e:
            if not supress_logs:
                log.debug(f"Error on http {method}, {client.base_url=} ({e!r})", exc_info=True)
            errors.append(e)
        await asyncio.sleep((1 + backoff_factor) ** i - 1)
    raise httpx.HTTPError(f"httpx {method} failed after {n_tries=}, {errors=}")


async def get_host_ip() -> str:
    ip_getter_service_urls = [
        "http://icanhazip.com",
        "http://ifconfig.me",
        "http://api.ipify.org",
        "http://bot.whatismyipaddress.com",
        "http://ipinfo.io/ip",
        "http://ipecho.net/plain",
    ]
    ips = await asyncio.gather(*(_get_text(url) for url in ip_getter_service_urls))
    counter = Counter(ip for ip in ips if re.match(r"^(?:\d{1,3}\.){3}\d{1,3}$", ip))
    return counter.most_common(1)[0][0]


async def _get_text(url) -> str:
    try:
        return (await get(url, follow_redirects=True, supress_logs=True)).text.strip()
    except Exception as e:
        log.debug(f"Error on ip getter service {url=} ({e!r})")
        return ""
