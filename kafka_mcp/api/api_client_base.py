"""Shared HTTP base client for the Apache Kafka (REST Proxy) API wrapper.

Strangled onto :class:`agent_utilities.http.BaseApiClient` (CONCEPT:AU-ECO.ui.fleet-http-client-library
Fleet HTTP Client Library): the public surface — constructor signature,
``request()`` return shapes, ``last_etag`` — is identical to the legacy
requests-based implementation, while the plumbing (base-URL joining, auth
injection, rate-limit capture, bounded 429 backoff, log redaction) now comes
from the shared fleet base.
"""

from typing import Any

import httpx
from agent_utilities.http import (
    AuthHeaderInjector,
    BaseApiClient,
    BasicAuth,
    TokenAuth,
)


class ApiClientBase:
    """Thin wrapper over the fleet HTTP base with token / basic-auth support.

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
        verify: bool = True,
        transport: httpx.BaseTransport | None = None,
    ):
        self.base_url = base_url.rstrip("/") + "/"
        self.token = token
        self.username = username
        self.password = password
        self.last_etag: str | None = None

        auth: AuthHeaderInjector | None = None
        if token:
            auth = TokenAuth(token)
        elif username and password:
            auth = BasicAuth(username, password)

        self._client = BaseApiClient(
            self.base_url,
            auth=auth,
            verify=verify,
            include_response_headers=True,
            transport=transport,
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
        if not endpoint.startswith("http"):
            endpoint = endpoint.lstrip("/")

        envelope = self._client.request(
            method,
            endpoint,
            params=params,
            data=data,
            json=json,
            content_type=content_type,
            accept=accept,
            headers=headers,
            raise_for_status=False,
        )

        self.last_etag = (envelope.get("headers") or {}).get("etag")

        status_code = envelope["status_code"]
        body = envelope["data"]
        if status_code >= 400:
            text = body if isinstance(body, str) else ("" if body is None else body)
            raise Exception(f"API error: {status_code} - {text}")

        if body is None or (isinstance(body, str) and not body.strip()):
            return {"status": "success"}
        if not isinstance(body, str):
            return body
        return {"status": "success", "text": body}
