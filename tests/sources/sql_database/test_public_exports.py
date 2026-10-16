import ast
from pathlib import Path

import dlt.sources.sql_database as sql_database
from dlt.common.libs.sql_alchemy import Table as AlchemyTable


def test_table_is_public_export() -> None:
    assert "Table" in sql_database.__all__
    from dlt.sources.sql_database import Table

    assert Table is AlchemyTable


def test_sql_database_pipeline_template_imports_are_public() -> None:
    template = Path("dlt/_workspace/_templates/_core_source_templates/sql_database_pipeline.py")
    tree = ast.parse(template.read_text())
    imported = [
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module == "dlt.sources.sql_database"
        for alias in node.names
    ]
    assert "Table" in imported
    missing = [name for name in imported if name not in sql_database.__all__]
    assert missing == []
