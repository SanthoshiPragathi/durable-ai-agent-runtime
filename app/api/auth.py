"""API-key authentication with two roles.

  operator   may read and write (create runs, send signals, fork, resolve a step).
  read-only  may only issue GET/HEAD/OPTIONS requests.

Keys come from API_OPERATOR_KEYS and API_READONLY_KEYS (comma-separated, so a key can be rotated by
listing the old and new one together). With BOTH empty authentication is OFF, which is the default so
local development and the demos work out of the box; the server logs a warning when that is the case.

Send the key as `X-API-Key: <key>` or `Authorization: Bearer <key>`. Keys are compared in constant
time. /health, /dashboard (the static page, which then asks for a key) and the OpenAPI docs stay open.
"""

import hmac

from fastapi import HTTPException, Request, status
from pydantic import SecretStr

from app.config import get_settings

SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}


def _split(secret: SecretStr) -> list[str]:
    return [k.strip() for k in secret.get_secret_value().split(",") if k.strip()]


def auth_enabled() -> bool:
    settings = get_settings()
    return bool(_split(settings.api_operator_keys) or _split(settings.api_readonly_keys))


def _presented_key(request: Request) -> str:
    key = request.headers.get("x-api-key")
    if key:
        return key.strip()
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    return token.strip() if scheme.lower() == "bearer" else ""


def _matches(presented: str, keys: list[str]) -> bool:
    # Check every key (no early exit) so timing does not reveal which one was close.
    results = [hmac.compare_digest(presented.encode(), key.encode()) for key in keys]
    return any(results)


async def require_api_key(request: Request) -> None:
    settings = get_settings()
    operator_keys, readonly_keys = _split(settings.api_operator_keys), _split(settings.api_readonly_keys)
    if not operator_keys and not readonly_keys:
        return  # authentication is off

    presented = _presented_key(request)
    is_operator = bool(presented) and _matches(presented, operator_keys)
    is_reader = bool(presented) and _matches(presented, readonly_keys)
    if not (is_operator or is_reader):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if request.method not in SAFE_METHODS and not is_operator:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="this key is read-only")
