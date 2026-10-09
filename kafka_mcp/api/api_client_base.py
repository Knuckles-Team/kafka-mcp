"""Shared HTTP base client for the Apache Kafka REST Proxy API wrapper."""

from __future__ import annotations

import json as _json
from base64 import b64encode
from typing import Any

import httpx
from agent_connector_sdk.tls.resolve import resolve_tls_profile


class ApiClientBase:
    """Thin wrapper over httpx with token / basic-auth support.

    The Confluent REST Proxy v3 API speaks JSON, but callers may pass an
    explicit ``content_type``/``accept`` (e.g. the versioned
    ``application/vnd.kafka.json.v2+json`` media types used by the v2
    consumer/producer endpoints) so this base never forces a single media
    type on every payload.
    """

    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        username: str | None = None,
        password: str | None = None,
        tls_profile: str | None = None,
        tls_profile_ref: str | None = None,
        transport: httpx.BaseTransport | None = None,
    ):
        self.base_url = base_url.rstrip("/") + "/"
        self.token = token
        self.username = username
        self.password = password
        self.last_etag: str | None = None

        default_headers: dict[str, str] = {}
        if token:
            default_headers["Authorization"] = f"Bearer {token}"
        elif username and password:
            basic = b64encode(f"{username}:{password}".encode()).decode()
            default_headers["Authorization"] = f"Basic {basic}"

        resolved = resolve_tls_profile(
            "kafka-rest",
            profile_name=tls_profile,
            profile_ref=tls_profile_ref,
        )

        self._client = httpx.Client(
            base_url=self.base_url,
            headers=default_headers,
            transport=transport,
            **resolved.httpx_kwargs(),
        )

    def request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        data: Any | None = None,
        json: Any | None = None,
        content_type: str | None = None,
        accept: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        """Perform an HTTP request and return parsed JSON, or raw text.

        Returns a dict when the response is JSON, otherwise
        ``{"status": "success", "text": <body>}``. Raises on HTTP >= 400.
        """
        if endpoint.startswith("http"):
            url = endpoint
        else:
            url = endpoint.lstrip("/")

        req_headers: dict[str, str] = dict(headers or {})
        if accept:
            req_headers["Accept"] = accept

        content: bytes | None = None
        if json is not None:
            content = _json.dumps(json).encode()
            req_headers["Content-Type"] = content_type or "application/json"
            data = None
        elif content_type:
            req_headers["Content-Type"] = content_type

        response = self._client.request(
            method,
            url,
            params=params,
            data=data,
            content=content,
            headers=req_headers or None,
        )

        self.last_etag = response.headers.get("etag")

        status_code = response.status_code
        if status_code >= 400:
            text = response.text
            raise Exception(f"API error: {status_code} - {text}")

        if status_code == 204 or not response.text.strip():
            return {"status": "success"}

        try:
            return response.json()
        except ValueError:
            return {"status": "success", "text": response.text}
