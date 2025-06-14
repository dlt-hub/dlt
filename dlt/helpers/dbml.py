import dlt
from dlt.common.exceptions import MissingDependencyException
from dlt.common.utils import without_none

try:
    from pydbml import PyDBML, Database
    from pydbml.classes import Table, Column, Reference, TableGroup, Project
except ModuleNotFoundError:
    raise MissingDependencyException("PyDBML", ["pydbml>=1.2.0"])


def _dlt_to_dbml_table(table) -> Table:
    return Table(
        name=table["name"],
        # schema=schema.name,
        note=table.get("description"),
        properties=without_none(
            {
                "resource": table.get("resource"),
                "write_disposition": table.get("write_disposition"),
            }
        ),  # type: ignore
    )


def _dlt_to_dbml_column(column) -> Column:
    return Column(
        name=column["name"],
        type=column["data_type"],
        unique=column.get("unique"),
        not_null=True if column.get("nullable") is False else False,
        pk=column.get("primary_key"),
        note=column.get("description"),
    )


def generate_dbml(schema: dlt.Schema) -> Database:
    db = Database()

    for table in schema.tables.values():
        dbml_table = _dlt_to_dbml_table(table)
        for column in table["columns"].values():
            dbml_column = _dlt_to_dbml_column(column)
            dbml_table.add_column(dbml_column)

        db.add_table(dbml_table)
        # TODO support Reference

    # db.add_table_group(TableGroup("dlt tables", schema.dlt_table_names()))
    # db.add_table_group(TableGroup("data tables", {t: None for t in schema.data_table_names()}))

    return db


if __name__ == "__main__":
    import sqlglot
    from sqlglot import exp as sge
    import pathlib
    import dlt
    from dlt.common.libs.sqlglot import ddl_to_table_schema

    dbml_path = pathlib.Path("/home/tjean/projects/dlthub/dlt/chess.dbml")
    schema_path = pathlib.Path("/home/tjean/projects/dlthub/dlt/chess.schema.json")

    if not dbml_path.exists():
        pipeline = dlt.pipeline("chess_pipeline")
        db = generate_dbml(pipeline.default_schema)
        dbml_path.write_text(db.dbml)
    else:
        parsed_dbml: Database = PyDBML(dbml_path)
        statements = sqlglot.parse(parsed_dbml.sql)

        schema = dlt.Schema("rebuilt_schema")
        for statement in statements:
            if not isinstance(statement, sge.Create):
                continue

            table_schema = ddl_to_table_schema(statement)
            schema.update_table(table_schema)

        schema_path.write_text(schema.to_pretty_json())
