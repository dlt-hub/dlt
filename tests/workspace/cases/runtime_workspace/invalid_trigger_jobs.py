"""A job whose trigger does not parse. Importing this module raises at decoration time."""

from dlt.hub.run import job


@job(trigger="every:5x")
def broken():
    return "never runs"
