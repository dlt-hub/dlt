def test_import_core_sources() -> None:
    # import all core sources with minimum deps
    from dlt.sources import filesystem, sql_database, rest_api
