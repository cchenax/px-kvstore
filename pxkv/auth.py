from __future__ import annotations

import base64
import hmac
from typing import Optional


ROLE_READER = "reader"
ROLE_WRITER = "writer"
ROLE_ADMIN = "admin"


def _role_rank(role: str) -> int:
    if role == ROLE_ADMIN:
        return 3
    if role == ROLE_WRITER:
        return 2
    if role == ROLE_READER:
        return 1
    return 0


def role_satisfies(granted: str, required: str) -> bool:
    return _role_rank(granted) >= _role_rank(required)


def _safe_eq(a: str, b: str) -> bool:
    return hmac.compare_digest(str(a), str(b))


def parse_bearer(authorization: str) -> Optional[str]:
    if not authorization:
        return None
    parts = authorization.split(" ", 1)
    if len(parts) != 2:
        return None
    scheme, value = parts[0].strip().lower(), parts[1].strip()
    if scheme != "bearer" or not value:
        return None
    return value


def parse_basic_password(authorization: str) -> Optional[str]:
    if not authorization:
        return None
    parts = authorization.split(" ", 1)
    if len(parts) != 2:
        return None
    scheme, value = parts[0].strip().lower(), parts[1].strip()
    if scheme != "basic" or not value:
        return None
    try:
        raw = base64.b64decode(value.encode("ascii"), validate=False).decode("utf-8", errors="replace")
    except Exception:
        return None
    if ":" in raw:
        return raw.split(":", 1)[1]
    return raw or None


def best_role_for_secret(
    secret: str,
    *,
    admin_token: str = "",
    writer_token: str = "",
    reader_token: str = "",
    admin_password: str = "",
    writer_password: str = "",
    reader_password: str = "",
) -> Optional[str]:
    if not secret:
        return None
    if admin_token and _safe_eq(secret, admin_token):
        return ROLE_ADMIN
    if admin_password and _safe_eq(secret, admin_password):
        return ROLE_ADMIN
    if writer_token and _safe_eq(secret, writer_token):
        return ROLE_WRITER
    if writer_password and _safe_eq(secret, writer_password):
        return ROLE_WRITER
    if reader_token and _safe_eq(secret, reader_token):
        return ROLE_READER
    if reader_password and _safe_eq(secret, reader_password):
        return ROLE_READER
    return None

