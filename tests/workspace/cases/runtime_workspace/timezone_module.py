"""Prints the context timezone, and the interval, when run as __main__."""

if __name__ == "__main__":
    from datetime import datetime

    import dlt
    from dlt.common.configuration.container import Container
    from dlt.common.configuration.specs.timezone_context import TimezoneContext
    from dlt.common.time import get_context_timezone, normalize_timezone

    # read before resolving the interval, which is what would create a `TimezoneContext`
    stored = normalize_timezone(datetime(2024, 1, 15, 23, 30), True)
    # `in` does not create a default instance, unlike `get`
    has_ctx = TimezoneContext in Container()

    interval = dlt.current.interval()
    print(f"tz={get_context_timezone()}")
    print(f"stored={stored.isoformat()}")
    print(f"tz_ctx={has_ctx}")
    print(f"interval={interval[0].isoformat() if interval else 'none'}")
