import pytest

pytest.importorskip("google.cloud.bigquery", reason="BigQuery dependencies not installed")
