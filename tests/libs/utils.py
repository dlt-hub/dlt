from base64 import b64decode


def decode_b64b64(value: str) -> str:
    try:
        return b64decode(b64decode(value)).decode("utf-8")
    except Exception as e:
        raise ValueError("Invalid double-base64 encoded secret") from e
