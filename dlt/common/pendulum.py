from datetime import timedelta  # noqa: I251
import pendulum  # noqa: I251

# force UTC as the local timezone to prevent local dates to be written to dbs
pendulum.set_local_timezone(pendulum.timezone('UTC'))


def __utcnow() -> pendulum.DateTime:
    """
    Use this function instead of datetime.now
    Returns:
        pendulum.DateTime -- current time in UTC timezone
    """
    return pendulum.now()


pendulum.utcnow = __utcnow  # type: ignore
