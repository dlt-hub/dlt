import pytest


pytest.importorskip("dlt.destinations.duckdb.duck", reason="duckdb dependencies are not installed")
