from __future__ import annotations

import threading
import time
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from krqs.config.settings import get_settings


class DartAPIError(Exception):
    pass


class TokenBucketRateLimiter:
    def __init__(self, rate_per_sec: float) -> None:
        self.rate = rate_per_sec
        self.capacity = max(int(rate_per_sec), 1)
        self.tokens = float(self.capacity)
        self.last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_refill = now
            if self.tokens < 1.0:
                wait_time = (1.0 - self.tokens) / self.rate
                time.sleep(wait_time)
                self.tokens = 0.0
            else:
                self.tokens -= 1.0


class DartClient:
    BASE_URL = "https://opendart.fss.or.kr/api"

    def __init__(
        self,
        api_key: str | None = None,
        rate_limit_per_sec: float | None = None,
        timeout: float = 30.0,
    ) -> None:
        settings = get_settings()
        self.api_key = api_key if api_key is not None else settings.dart_api_key
        if not self.api_key:
            raise ValueError(
                "DART_API_KEY is not configured. Set via .env or constructor."
            )
        rate = (
            rate_limit_per_sec
            if rate_limit_per_sec is not None
            else settings.dart_rate_limit_per_sec
        )
        self._limiter = TokenBucketRateLimiter(rate)
        self._timeout = timeout
        self._client = httpx.Client(base_url=self.BASE_URL, timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> DartClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=3, max=60),
        retry=retry_if_exception_type(
            (httpx.TransportError, httpx.HTTPStatusError, ConnectionError)
        ),
        reraise=True,
    )
    def _get_json(
        self, path: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        self._limiter.acquire()
        merged = {"crtfc_key": self.api_key, **params}
        try:
            resp = self._client.get(path, params=merged)
        except httpx.TransportError:
            # Re-create client on connection reset to get a fresh socket
            self._client.close()
            self._client = httpx.Client(
                base_url=self.BASE_URL, timeout=self._timeout
            )
            raise
        resp.raise_for_status()
        data = resp.json()
        status = str(data.get("status", ""))
        # 000: 정상, 013: 조회 데이터 없음
        if status not in ("000", "013"):
            raise DartAPIError(
                f"DART API status={status}: {data.get('message', '')}"
            )
        return data

    def fetch_corp_code_zip(self) -> bytes:
        self._limiter.acquire()
        resp = self._client.get(
            "/corpCode.xml", params={"crtfc_key": self.api_key}
        )
        resp.raise_for_status()
        return resp.content

    def fetch_single_company_financials(
        self,
        corp_code: str,
        bsns_year: int,
        reprt_code: str = "11011",
        fs_div: str = "CFS",
    ) -> dict[str, Any]:
        return self._get_json(
            "/fnlttSinglAcntAll.json",
            {
                "corp_code": corp_code,
                "bsns_year": str(bsns_year),
                "reprt_code": reprt_code,
                "fs_div": fs_div,
            },
        )
