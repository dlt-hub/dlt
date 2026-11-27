from typing import Any, Dict, Mapping, Union, cast

from dlt._workspace.deployment.typing import TRetrySpec

DEFAULT_RETRY_INITIAL_DELAY = 30
DEFAULT_RETRY_BACKOFF = 2.0
DEFAULT_RETRY_MAX_DELAY = 3600
DEFAULT_RETRY_JITTER = True
MAX_JOB_RETRIES = 10

_RETRY_FIELDS = frozenset({"max_retries", "initial_delay", "backoff", "max_delay", "jitter"})


def _retry_number(name: str, value: Any, unit: str) -> Union[int, float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"retry.{name} must be a {unit}. Got {value!r}.")
    return cast(Union[int, float], value)


def normalize_retry(retry: Union[int, Mapping[str, Any]]) -> TRetrySpec:
    """Expand a decorator `retry` argument into the `execute.retry` mapping.

    An int is `max_retries`. A mapping must include `max_retries`. Fields left
    out take the defaults. `max_retries` is an integer from 0 to 10.

    Raises:
        ValueError: `retry` is not an int or a mapping, or a field is invalid.
    """
    if isinstance(retry, Mapping):
        fields = dict(retry)
        unexpected = sorted(set(fields) - _RETRY_FIELDS)
        if unexpected:
            listed = ", ".join(unexpected)
            raise ValueError(
                f"retry received unexpected fields: {listed}. Use max_retries, initial_delay,"
                " backoff, max_delay, jitter."
            )
    elif isinstance(retry, int) and not isinstance(retry, bool):
        fields = {"max_retries": retry}
    elif isinstance(retry, (bool, float)):
        raise ValueError(
            f"retry.max_retries must be an integer from 0 to {MAX_JOB_RETRIES}. Got {retry!r}."
        )
    else:
        raise ValueError(f"retry must be an integer or a mapping with max_retries. Got {retry!r}.")
    if "max_retries" not in fields:
        raise ValueError("retry requires max_retries, an integer from 0 to 10.")
    max_retries = fields["max_retries"]
    if (
        isinstance(max_retries, bool)
        or not isinstance(max_retries, int)
        or not 0 <= max_retries <= MAX_JOB_RETRIES
    ):
        raise ValueError(
            f"retry.max_retries must be an integer from 0 to {MAX_JOB_RETRIES}. Got"
            f" {max_retries!r}."
        )
    jitter = fields.get("jitter", DEFAULT_RETRY_JITTER)
    if not isinstance(jitter, bool):
        raise ValueError(f"retry.jitter must be a bool. Got {jitter!r}.")
    return {
        "max_retries": max_retries,
        "initial_delay": _retry_number(
            "initial_delay",
            fields.get("initial_delay", DEFAULT_RETRY_INITIAL_DELAY),
            "number of seconds",
        ),
        "backoff": _retry_number("backoff", fields.get("backoff", DEFAULT_RETRY_BACKOFF), "number"),
        "max_delay": _retry_number(
            "max_delay", fields.get("max_delay", DEFAULT_RETRY_MAX_DELAY), "number of seconds"
        ),
        "jitter": jitter,
    }
