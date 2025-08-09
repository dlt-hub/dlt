from datetime import timedelta, timezone  # noqa: I251
import pendulum  # noqa: I251


def __utcnow() -> pendulum.DateTime:
    """
    Use this function instead of datetime.now
    Returns:
        pendulum.DateTime -- current time in UTC timezone
    """
    return pendulum.now()


pendulum.utcnow = __utcnow  # type: ignore
