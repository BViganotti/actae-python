"""Stable cross-SDK identifiers used by Actae's ergonomic runtime."""

import hashlib
import uuid
from typing import Any


_ACTAE_NAMESPACE = uuid.UUID("3f74e5f1-9b2c-4a7e-8f1d-000000000001")
_NAME_BYTE_CAP = 256


def deterministic_operation_key(scope: str, action: str, *identity: Any) -> str:
    """Return the shared Python/TypeScript/Go deterministic UUIDv5.

    Parts are NUL-separated, so different part boundaries cannot collide.
    The UTF-8 input is capped at 256 bytes exactly like the other SDKs.
    """

    if not isinstance(scope, str) or not scope:
        raise ValueError("scope must be a non-empty string")
    if not isinstance(action, str) or not action:
        raise ValueError("action must be a non-empty string")
    parts = [scope, action]
    for value in identity:
        if value is None:
            parts.append("")
        elif isinstance(value, str):
            parts.append(value)
        else:
            parts.append(str(value))
    name = "\x00".join(parts).encode("utf-8")[:_NAME_BYTE_CAP]

    digest = hashlib.sha1(_ACTAE_NAMESPACE.bytes + name).digest()
    raw = bytearray(digest[:16])
    raw[6] = (raw[6] & 0x0F) | 0x50
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))
