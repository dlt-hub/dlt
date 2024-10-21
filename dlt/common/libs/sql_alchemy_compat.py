try:
    import sqlalchemy
except ImportError:
    from dlt.common.libs.sql_alchemy_shims import URL, make_url
else:
    from sqlalchemy.engine import URL, make_url  # type: ignore[assignment]
