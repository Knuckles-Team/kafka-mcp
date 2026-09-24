"""Local port of ``agent_utilities.httpsupport`` onto ``agent_connector_sdk``.

SDK-GAP (EH-48x, see SDK-GAPS.md #1): agent-connector-sdk has no equivalent of
agent_utilities.httpsupport (the shared fleet HTTP client library -- 2000+
lines: BaseApiClient/AsyncBaseApiClient, TokenAuth/BasicAuth/QueryApiKeyAuth,
5-dialect pagination, rate-limit telemetry, log redaction). Porting all of it
into every connector is out of scope for this migration; this module is a
compact, hand-verified local port covering exactly what this package's own
api_client_base.py uses: the request/get/post/put/patch/delete envelope
contract, the three auth strategies (byte-identical to the original -- they
have zero agent_utilities dependency of their own), and TLS-profile
resolution via agent_connector_sdk.tls. It is intentionally NOT a full port:

Dropped relative to the original (see SDK-GAPS.md #1 for the proposal to
port httpsupport into the SDK itself, which would remove modules like this
one from every connector that vendors it):
* PaginationIterator / AsyncPaginationIterator (5-dialect pagination) --
  ``.paginate()`` raises NotImplementedError instead of iterating.
* RateLimitCapture telemetry -- ``envelope["rate_limit"]`` is always ``None``.
* Log redaction by default (LogRedactor on the module logger).
* The transparent one-retry-after-401 hook (``_should_refresh_auth``).

Kept: the envelope shape (``{"status_code", "data", "rate_limit", ["headers"]}``),
bounded 429 backoff via ``Retry-After``, status -> exception mapping, and
``guard_destructive``.
"""

from __future__ import annotations

import time
from base64 import b64encode
from collections.abc import Callable
from typing import Any

import httpx

from agent_connector_sdk.exceptions import (
    ApiError,
    AuthError,
    ParameterError,
    UnauthorizedError,
)
from agent_connector_sdk.http.client import create_http_client
from agent_connector_sdk.http.options import HttpClientOptions
from agent_connector_sdk.tls.resolve import resolve_tls_profile

__all__ = [
    "AsyncBaseApiClient",
    "AuthHeaderInjector",
    "BaseApiClient",
    "BasicAuth",
    "DestructiveOperationError",
    "QueryApiKeyAuth",
    "TokenAuth",
]

JSON_CONTENT_TYPE = "application/json"
DEFAULT_MAX_RETRIES_429 = 3
DEFAULT_RETRY_AFTER_CAP_S = 30.0
DEFAULT_HTTP_TIMEOUT_S = 30.0

DEFAULT_ERROR_MAP: dict[int, type[Exception]] = {
    400: ParameterError,
    401: AuthError,
    403: UnauthorizedError,
    404: ParameterError,
}


class DestructiveOperationError(PermissionError):
    """Raised when a destructive operation is attempted while gated off."""


# --------------------------------------------------------------------- #
# Auth strategies -- byte-identical in behavior to
# agent_utilities.httpsupport.auth (that module had zero agent_utilities
# dependencies of its own beyond living in the same package).
# --------------------------------------------------------------------- #
class AuthHeaderInjector:
    """Base auth strategy: contributes headers / query params per request."""

    def headers(self) -> dict[str, str]:
        return {}

    def params(self) -> dict[str, str]:
        return {}

    def secrets(self) -> list[str]:
        return []


class TokenAuth(AuthHeaderInjector):
    """Header-token auth with a configurable header name and scheme prefix."""

    def __init__(
        self,
        token: str | None = None,
        *,
        token_provider: Callable[[], str] | None = None,
        header: str = "Authorization",
        prefix: str | None = "Bearer",
    ) -> None:
        if (token is None) == (token_provider is None):
            raise ValueError(
                "TokenAuth requires exactly one of 'token' or 'token_provider'"
            )
        self._token = token
        self._token_provider = token_provider
        self.header = header
        self.prefix = prefix or ""

    def _current_token(self) -> str:
        if self._token_provider is not None:
            return self._token_provider()
        return self._token or ""

    def headers(self) -> dict[str, str]:
        token = self._current_token()
        value = f"{self.prefix} {token}" if self.prefix else token
        return {self.header: value}

    def secrets(self) -> list[str]:
        return [self._token] if self._token else []


class BasicAuth(AuthHeaderInjector):
    """RFC 7617 ``Authorization: Basic`` username/password credentials."""

    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self._password = password

    def headers(self) -> dict[str, str]:
        credentials = b64encode(f"{self.username}:{self._password}".encode()).decode(
            "ascii"
        )
        return {"Authorization": f"Basic {credentials}"}

    def secrets(self) -> list[str]:
        return [self._password] if self._password else []


class QueryApiKeyAuth(AuthHeaderInjector):
    """API key carried as a query parameter on every request."""

    def __init__(self, param: str, key: str) -> None:
        if not param or not key:
            raise ValueError("QueryApiKeyAuth requires both 'param' and 'key'")
        self.param = param
        self._key = key

    def params(self) -> dict[str, str]:
        return {self.param: self._key}

    def secrets(self) -> list[str]:
        return [self._key]


# --------------------------------------------------------------------- #
# Shared request/envelope/error-mapping core
# --------------------------------------------------------------------- #
class _ApiClientCore:
    def _init_core(
        self,
        base_url: str,
        *,
        auth: AuthHeaderInjector | None,
        headers: dict[str, str] | None,
        timeout: float,
        max_retries_429: int,
        retry_after_cap_s: float,
        error_map: dict[int, type[Exception]] | None,
        default_error: type[Exception],
        allow_destructive: bool,
        include_response_headers: bool,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries_429 = max_retries_429
        self.retry_after_cap_s = retry_after_cap_s
        self.allow_destructive = allow_destructive
        self.include_response_headers = include_response_headers
        self.error_map: dict[int, type[Exception]] = {
            **DEFAULT_ERROR_MAP,
            **(error_map or {}),
        }
        self.default_error = default_error
        self._auth = auth or AuthHeaderInjector()

    def default_headers(self) -> dict[str, str]:
        return {"Accept": JSON_CONTENT_TYPE}

    def _merged_headers(
        self,
        headers: dict[str, str] | None,
        content_type: str | None,
        accept: str | None,
    ) -> dict[str, str]:
        merged = {
            **self.default_headers(),
            **self._auth.headers(),
            **(headers or {}),
        }
        if content_type:
            merged["Content-Type"] = content_type
        if accept:
            merged["Accept"] = accept
        return merged

    def _merged_params(self, params: dict[str, Any] | None) -> dict[str, Any] | None:
        merged = {
            **{k: v for k, v in (params or {}).items() if v is not None},
            **self._auth.params(),
        }
        return merged or None

    def _envelope(self, response: httpx.Response, data: Any) -> dict[str, Any]:
        envelope: dict[str, Any] = {
            "status_code": response.status_code,
            "data": data,
            "rate_limit": None,  # SDK-GAP: rate-limit telemetry not ported
        }
        if self.include_response_headers:
            envelope["headers"] = dict(response.headers)
        return envelope

    @staticmethod
    def _parse_body(response: httpx.Response) -> Any:
        if response.status_code == 204 or not response.content:
            return None
        content_type = response.headers.get("Content-Type", "")
        if "json" in content_type:
            try:
                return response.json()
            except ValueError:
                return response.text
        return response.text

    def _map_error(self, response: httpx.Response, data: Any) -> Exception:
        detail = ""
        if isinstance(data, dict):
            detail = str(
                data.get("detail")
                or data.get("message")
                or data.get("error")
                or data.get("errorSummary")
                or ""
            )
        message = (
            f"HTTP {response.status_code} for "
            f"{response.request.method} {response.request.url.path}"
        )
        if detail:
            message = f"{message}: {detail}"
        exc_class = self.error_map.get(response.status_code, self.default_error)
        return exc_class(message)

    def guard_destructive(self, operation: str) -> None:
        if not self.allow_destructive:
            raise DestructiveOperationError(
                f"Destructive operation {operation!r} is disabled. "
                "Construct the client with allow_destructive=True to enable it."
            )

    def _retry_delay(self, response: httpx.Response, attempts: int) -> float | None:
        if response.status_code != 429 or attempts >= self.max_retries_429:
            return None
        retry_after = response.headers.get("Retry-After")
        try:
            delay = float(retry_after) if retry_after else 1.0
        except ValueError:
            delay = 1.0
        return min(max(delay, 0.0), self.retry_after_cap_s)


def _resolve_tls(
    tls_service: str, tls_profile: str | None, tls_profile_ref: str | None
):
    if not tls_profile and not tls_profile_ref:
        return None
    return resolve_tls_profile(
        tls_service, profile_name=tls_profile, profile_ref=tls_profile_ref
    )


class BaseApiClient(_ApiClientCore):
    """Synchronous fleet API client base -- see module docstring for parity notes."""

    def __init__(
        self,
        base_url: str,
        *,
        auth: AuthHeaderInjector | None = None,
        headers: dict[str, str] | None = None,
        tls_service: str = "fleet-http",
        tls_profile: str | None = None,
        tls_profile_ref: str | None = None,
        timeout: float = DEFAULT_HTTP_TIMEOUT_S,
        retry: Any | None = None,  # SDK-GAP: ResiliencePolicy not ported; ignored
        max_retries_429: int = DEFAULT_MAX_RETRIES_429,
        retry_after_cap_s: float = DEFAULT_RETRY_AFTER_CAP_S,
        error_map: dict[int, type[Exception]] | None = None,
        default_error: type[Exception] = ApiError,
        allow_destructive: bool = False,
        include_response_headers: bool = False,
        transport: httpx.BaseTransport | None = None,
        **_ignored: Any,
    ) -> None:
        self._init_core(
            base_url,
            auth=auth,
            headers=headers,
            timeout=timeout,
            max_retries_429=max_retries_429,
            retry_after_cap_s=retry_after_cap_s,
            error_map=error_map,
            default_error=default_error,
            allow_destructive=allow_destructive,
            include_response_headers=include_response_headers,
        )
        tls = _resolve_tls(tls_service, tls_profile, tls_profile_ref)
        self._client = create_http_client(
            HttpClientOptions(
                base_url=self.base_url, timeout=timeout, tls=tls, allow_plaintext=True
            ),
            transport=transport,
        )

    def _send(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        data: Any | None = None,
        content: bytes | str | None = None,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        accept: str | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        merged_params = self._merged_params(params)
        attempts = 0
        while True:
            response = self._client.request(
                method,
                endpoint,
                params=merged_params,
                json=json,
                data=data,
                content=content,
                headers=self._merged_headers(headers, content_type, accept),
                timeout=(timeout if timeout is not None else httpx.USE_CLIENT_DEFAULT),
            )
            delay = self._retry_delay(response, attempts)
            if delay is not None:
                attempts += 1
                time.sleep(delay)
                continue
            return response

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        data: Any | None = None,
        content: bytes | str | None = None,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        accept: str | None = None,
        timeout: float | None = None,
        raise_for_status: bool = True,
    ) -> dict[str, Any]:
        response = self._send(
            method,
            endpoint,
            params=params,
            json=json,
            data=data,
            content=content,
            headers=headers,
            content_type=content_type,
            accept=accept,
            timeout=timeout,
        )
        body = self._parse_body(response)
        if raise_for_status and response.status_code >= 400:
            raise self._map_error(response, body)
        return self._envelope(response, body)

    def get(self, endpoint: str, **kwargs: Any) -> dict[str, Any]:
        return self.request("GET", endpoint, **kwargs)

    def post(self, endpoint: str, **kwargs: Any) -> dict[str, Any]:
        return self.request("POST", endpoint, **kwargs)

    def put(self, endpoint: str, **kwargs: Any) -> dict[str, Any]:
        return self.request("PUT", endpoint, **kwargs)

    def patch(self, endpoint: str, **kwargs: Any) -> dict[str, Any]:
        return self.request("PATCH", endpoint, **kwargs)

    def delete(self, endpoint: str, **kwargs: Any) -> dict[str, Any]:
        return self.request("DELETE", endpoint, **kwargs)

    def head(self, endpoint: str, **kwargs: Any) -> dict[str, Any]:
        return self.request("HEAD", endpoint, **kwargs)

    def paginate(self, endpoint: str, **options: Any) -> Any:
        raise NotImplementedError(
            "paginate(): PaginationIterator was not ported from "
            "agent_utilities.httpsupport -- see SDK-GAPS.md #1"
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> BaseApiClient:
        return self

    def __exit__(self, *_exc_info: Any) -> None:
        self.close()


class AsyncBaseApiClient(_ApiClientCore):
    """Asynchronous fleet API client base -- see module docstring for parity notes."""

    def __init__(
        self,
        base_url: str,
        *,
        auth: AuthHeaderInjector | None = None,
        headers: dict[str, str] | None = None,
        tls_service: str = "fleet-http",
        tls_profile: str | None = None,
        tls_profile_ref: str | None = None,
        timeout: float = DEFAULT_HTTP_TIMEOUT_S,
        retry: Any | None = None,  # SDK-GAP: ResiliencePolicy not ported; ignored
        max_retries_429: int = DEFAULT_MAX_RETRIES_429,
        retry_after_cap_s: float = DEFAULT_RETRY_AFTER_CAP_S,
        error_map: dict[int, type[Exception]] | None = None,
        default_error: type[Exception] = ApiError,
        allow_destructive: bool = False,
        include_response_headers: bool = False,
        transport: httpx.AsyncBaseTransport | None = None,
        **_ignored: Any,
    ) -> None:
        self._init_core(
            base_url,
            auth=auth,
            headers=headers,
            timeout=timeout,
            max_retries_429=max_retries_429,
            retry_after_cap_s=retry_after_cap_s,
            error_map=error_map,
            default_error=default_error,
            allow_destructive=allow_destructive,
            include_response_headers=include_response_headers,
        )
        tls = _resolve_tls(tls_service, tls_profile, tls_profile_ref)
        from agent_connector_sdk.http.client import create_async_http_client

        self._client = create_async_http_client(
            HttpClientOptions(
                base_url=self.base_url, timeout=timeout, tls=tls, allow_plaintext=True
            ),
            transport=transport,
        )

    async def _send(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        data: Any | None = None,
        content: bytes | str | None = None,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        accept: str | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        import anyio

        merged_params = self._merged_params(params)
        attempts = 0
        while True:
            response = await self._client.request(
                method,
                endpoint,
                params=merged_params,
                json=json,
                data=data,
                content=content,
                headers=self._merged_headers(headers, content_type, accept),
                timeout=(timeout if timeout is not None else httpx.USE_CLIENT_DEFAULT),
            )
            delay = self._retry_delay(response, attempts)
            if delay is not None:
                attempts += 1
                await anyio.sleep(delay)
                continue
            return response

    async def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        data: Any | None = None,
        content: bytes | str | None = None,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        accept: str | None = None,
        timeout: float | None = None,
        raise_for_status: bool = True,
    ) -> dict[str, Any]:
        response = await self._send(
            method,
            endpoint,
            params=params,
            json=json,
            data=data,
            content=content,
            headers=headers,
            content_type=content_type,
            accept=accept,
            timeout=timeout,
        )
        body = self._parse_body(response)
        if raise_for_status and response.status_code >= 400:
            raise self._map_error(response, body)
        return self._envelope(response, body)

    async def get(self, endpoint: str, **kwargs: Any) -> dict[str, Any]:
        return await self.request("GET", endpoint, **kwargs)

    async def post(self, endpoint: str, **kwargs: Any) -> dict[str, Any]:
        return await self.request("POST", endpoint, **kwargs)

    async def put(self, endpoint: str, **kwargs: Any) -> dict[str, Any]:
        return await self.request("PUT", endpoint, **kwargs)

    async def patch(self, endpoint: str, **kwargs: Any) -> dict[str, Any]:
        return await self.request("PATCH", endpoint, **kwargs)

    async def delete(self, endpoint: str, **kwargs: Any) -> dict[str, Any]:
        return await self.request("DELETE", endpoint, **kwargs)

    async def head(self, endpoint: str, **kwargs: Any) -> dict[str, Any]:
        return await self.request("HEAD", endpoint, **kwargs)

    def paginate(self, endpoint: str, **options: Any) -> Any:
        raise NotImplementedError(
            "paginate(): AsyncPaginationIterator was not ported from "
            "agent_utilities.httpsupport -- see SDK-GAPS.md #1"
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> AsyncBaseApiClient:
        return self

    async def __aexit__(self, *_exc_info: Any) -> None:
        await self.close()
