"""Unit tests for DuckLakeSqlClient.build_attach_statement.

Tests the attach statement generated for each catalog drivername, ensuring
DuckDB-backed catalogs do not receive META_TYPE 'sqlite'.
"""
import pytest
from dlt.common.configuration.specs.connection_string_credentials import (
    ConnectionStringCredentials,
)
from dlt.destinations.impl.ducklake.sql_client import DuckLakeSqlClient


STORAGE_URL = "s3://bucket/prefix/"


def _catalog(uri: str) -> ConnectionStringCredentials:
    c = ConnectionStringCredentials(uri)
    c.resolve()
    return c


def test_sqlite_catalog_includes_meta_type_sqlite():
    stmt = DuckLakeSqlClient.build_attach_statement(
        ducklake_name="mydb",
        catalog=_catalog("sqlite:///catalog.sqlite"),
        storage_url=STORAGE_URL,
    )
    assert "META_TYPE 'sqlite'" in stmt
    assert "META_JOURNAL_MODE 'WAL'" in stmt
    assert "ducklake:catalog.sqlite" in stmt


def test_duckdb_catalog_excludes_meta_type_sqlite():
    """DuckDB-backed catalogs must not receive META_TYPE 'sqlite'.

    Prior to the fix, drivername='duckdb' was treated identically to 'sqlite',
    causing 'PRAGMA journal_mode=WAL: file is not a database' on DuckDB files.
    """
    stmt = DuckLakeSqlClient.build_attach_statement(
        ducklake_name="mydb",
        catalog=_catalog("duckdb:///catalog.duckdb"),
        storage_url=STORAGE_URL,
    )
    assert "META_TYPE" not in stmt
    assert "META_JOURNAL_MODE" not in stmt
    assert "ducklake:catalog.duckdb" in stmt
    assert f"DATA_PATH '{STORAGE_URL}'" in stmt


def test_duckdb_catalog_attach_format():
    stmt = DuckLakeSqlClient.build_attach_statement(
        ducklake_name="mydb",
        catalog=_catalog("duckdb:///catalog.duckdb"),
        storage_url=STORAGE_URL,
    )
    assert (
        stmt
        == f"ATTACH IF NOT EXISTS 'ducklake:catalog.duckdb' AS mydb (DATA_PATH '{STORAGE_URL}')"
    )
