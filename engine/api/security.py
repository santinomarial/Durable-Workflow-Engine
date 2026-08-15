"""Fail-closed API-key authentication and request protection."""

from __future__ import annotations

import hashlib
import hmac
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Literal, cast

from fastapi import HTTPException, Request, status
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from engine.config import positive_int, secret_value

Role = Literal["viewer", "operator", "admin"]
ROLE_LEVEL: dict[Role, int] = {"viewer": 10, "operator": 20, "admin": 30}
PUBLIC_PATHS = frozenset(
    {
        "/",
        "/api/health",
        "/api/health/live",
        "/api/health/ready",
        "/docs",
        "/redoc",
        "/openapi.json",
    }
)


@dataclass(frozen=True, slots=True)
class APIKey:
    key_id: str
    role: Role
    digest: str

    @classmethod
    def from_token(cls, key_id: str, role: Role, token: str) -> APIKey:
        """Build a key for tests or local embedding without persisting the token."""
        if len(token) < 16:
            raise ValueError("API tokens must contain at least 16 characters")
        return cls(key_id=key_id, role=role, digest=hashlib.sha256(token.encode()).hexdigest())


@dataclass(frozen=True, slots=True)
class Principal:
    key_id: str
    role: Role


@dataclass(frozen=True, slots=True)
class AuthConfig:
    keys: tuple[APIKey, ...]
    disabled: bool = False
    requests_per_minute: int = 300
    max_request_bytes: int = 1_048_576

    @classmethod
    def from_env(cls) -> AuthConfig:
        mode = os.environ.get("DWE_AUTH_MODE", "required").strip().lower()
        if mode not in {"required", "disabled"}:
            raise RuntimeError("DWE_AUTH_MODE must be 'required' or 'disabled'")
        raw_keys = secret_value("DWE_API_KEYS") or ""
        keys = tuple(_parse_key(value) for value in raw_keys.split(",") if value.strip())
        config = cls(
            keys=keys,
            disabled=mode == "disabled",
            requests_per_minute=positive_int("DWE_RATE_LIMIT_PER_MINUTE", 300),
            max_request_bytes=positive_int("DWE_MAX_REQUEST_BYTES", 1_048_576),
        )
        return config

    @classmethod
    def insecure_for_testing(cls) -> AuthConfig:
        return cls(keys=(), disabled=True)

    def validate(self) -> None:
        if self.disabled and self.keys:
            raise RuntimeError("DWE_API_KEYS cannot be set when authentication is disabled")
        if not self.disabled and not self.keys:
            raise RuntimeError(
                "authentication is required: configure DWE_API_KEYS or explicitly set "
                "DWE_AUTH_MODE=disabled for isolated development"
            )
        key_ids = [key.key_id for key in self.keys]
        digests = [key.digest for key in self.keys]
        if len(key_ids) != len(set(key_ids)):
            raise RuntimeError("DWE_API_KEYS contains a duplicate key ID")
        if len(digests) != len(set(digests)):
            raise RuntimeError("DWE_API_KEYS contains a duplicate token digest")

    def authenticate(self, token: str) -> Principal | None:
        if self.disabled:
            return Principal(key_id="development", role="admin")
        digest = hashlib.sha256(token.encode()).hexdigest()
        match: APIKey | None = None
        for key in self.keys:
            if hmac.compare_digest(digest, key.digest):
                match = key
        return Principal(match.key_id, match.role) if match is not None else None


def _parse_key(value: str) -> APIKey:
    parts = value.strip().split(":")
    if len(parts) != 3:
        raise RuntimeError("each DWE_API_KEYS entry must use key-id:role:sha256-digest")
    key_id, raw_role, digest = parts
    if not key_id or len(key_id) > 100 or not key_id.replace("-", "").replace("_", "").isalnum():
        raise RuntimeError("API key IDs must be 1-100 letters, numbers, hyphens, or underscores")
    if raw_role not in ROLE_LEVEL:
        raise RuntimeError("API key roles must be viewer, operator, or admin")
    normalized_digest = digest.lower()
    if len(normalized_digest) != 64 or any(
        character not in "0123456789abcdef" for character in normalized_digest
    ):
        raise RuntimeError("API key digests must be 64-character SHA-256 hex values")
    return APIKey(key_id=key_id, role=raw_role, digest=normalized_digest)


def bearer_token(request: Request) -> str | None:
    header = request.headers.get("Authorization", "")
    scheme, separator, token = header.partition(" ")
    if not separator or scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()


def require_role(request: Request, minimum: Role) -> Principal:
    principal = cast(Principal | None, getattr(request.state, "principal", None))
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="authentication required"
        )
    if ROLE_LEVEL[principal.role] < ROLE_LEVEL[minimum]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"{minimum} role required",
        )
    return principal


class FixedWindowRateLimiter:
    """Bound per-process request memory and enforce a principal-level limit."""

    def __init__(self, requests_per_minute: int) -> None:
        self._limit = requests_per_minute
        self._requests: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, identity: str, now: float | None = None) -> tuple[bool, int]:
        current = time.monotonic() if now is None else now
        cutoff = current - 60
        requests = self._requests[identity]
        while requests and requests[0] <= cutoff:
            requests.popleft()
        if len(requests) >= self._limit:
            retry_after = max(1, int(60 - (current - requests[0])))
            return False, retry_after
        requests.append(current)
        return True, 0


class RequestBodyLimitMiddleware:
    """Reject oversized fixed-length and streaming request bodies."""

    def __init__(self, app: ASGIApp, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        content_length = headers.get(b"content-length")
        if content_length is not None:
            try:
                if int(content_length) > self.max_bytes:
                    await self._reject(scope, receive, send)
                    return
            except ValueError:
                response = JSONResponse({"detail": "invalid Content-Length"}, status_code=400)
                await response(scope, receive, send)
                return

        received = 0

        async def limited_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    raise RequestBodyTooLarge
            return message

        try:
            await self.app(scope, limited_receive, send)
        except RequestBodyTooLarge:
            await self._reject(scope, receive, send)

    async def _reject(self, scope: Scope, receive: Receive, send: Send) -> None:
        response = JSONResponse(
            {"detail": f"request body exceeds {self.max_bytes} bytes"},
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
        )
        await response(scope, receive, send)


class RequestBodyTooLarge(Exception):
    pass
