from typing import Any, Dict


def serialize(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {k: serialize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [serialize(v) for v in value]
    try:
        if hasattr(value, "__dict__"):
            attrs = vars(value)
            if attrs:
                return serialize(attrs)
        return str(value)
    except Exception:
        return repr(value)


def bearer_auth(token: str) -> Dict[str, str]:
    return {"authorization": f"Bearer {token}"}
