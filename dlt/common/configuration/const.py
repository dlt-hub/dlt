from dlt.common.pendulum import pendulum

RANDOM_DATE = pendulum.datetime(1768, 7, 21, 2, 56, 7, 3, tz="UTC")
TYPE_EXAMPLES = {
    "text": "<configure me>",
    "timestamp": RANDOM_DATE.to_iso8601_string(),
    "date": RANDOM_DATE.to_date_string(),
}
