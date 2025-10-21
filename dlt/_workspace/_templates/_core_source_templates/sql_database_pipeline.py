# flake8: noqa
import humanize
from typing import Any
import os

import dlt
from dlt.common import pendulum
from dlt.sources.credentials import ConnectionStringCredentials

from dlt.sources.sql_database import sql_database, sql_table, Table

from sqlalchemy.sql.sqltypes import TypeEngine
import sqlalchemy as sa


def load_select_tables_from_database() -> None:
    """Use the sql_database source to reflect an entire database schema and load select tables from it.

    This example sources data from the public Rfam MySQL database.
    """
    # Create a pipeline
    pipeline = dlt.pipeline(pipeline_name="rfam", destination="duckdb", dataset_name="rfam_data")

    # Credentials for the sample database.
    # Note: It is recommended to configure credentials in `.dlt/secrets.toml` under `sources.sql_database.credentials`
    credentials = ConnectionStringCredentials(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    )
    # To pass the credentials from `secrets.toml`, comment out the above credentials.
    # And the credentials will be automatically read from `secrets.toml`.

    # Configure the source to load a few select tables incrementally
    source_1 = sql_database(credentials).with_resources("family", "clan")

    # Add incremental config to the resources. "updated" is a timestamp column in these tables that gets used as a cursor
    source_1.family.apply_hints(incremental=dlt.sources.incremental("updated"))
    source_1.clan.apply_hints(incremental=dlt.sources.incremental("updated"))

    # Run the pipeline. The merge write disposition merges existing rows in the destination by primary key
    info = pipeline.run(source_1, write_disposition="merge")
    print(info)

    # Load some other tables with replace write disposition. This overwrites the existing tables in destination
    source_2 = sql_database(credentials).with_resources("features", "author")
    info = pipeline.run(source_2, write_disposition="replace")
    print(info)

    # Load a table incrementally with append write disposition
    # this is good when a table only has new rows inserted, but not updated
    source_3 = sql_database(credentials).with_resources("genome")
    source_3.genome.apply_hints(incremental=dlt.sources.incremental("created"))

    info = pipeline.run(source_3, write_disposition="append")
    print(info)


def load_entire_database() -> None:
    """Use the sql_database source to completely load all tables in a database"""
    pipeline = dlt.pipeline(pipeline_name="rfam", destination="duckdb", dataset_name="rfam_data")

    # By default the sql_database source reflects all tables in the schema
    # The database credentials are sourced from the `.dlt/secrets.toml` configuration
    source = sql_database()

    # Run the pipeline. For a large db this may take a while
    info = pipeline.run(source, write_disposition="replace")
    print(humanize.precisedelta(pipeline.last_trace.finished_at - pipeline.last_trace.started_at))
    print(info)


def load_standalone_table_resource() -> None:
    """Load a few known tables with the standalone sql_table resource, request full schema and deferred
    table reflection"""
    pipeline = dlt.pipeline(
        pipeline_name="rfam_database",
        destination="duckdb",
        dataset_name="rfam_data",
        dev_mode=True,
    )

    # Load a table incrementally starting at a given date
    # Adding incremental via argument like this makes extraction more efficient
    # as only rows newer than the start date are fetched from the table
    # we also use `detect_precision_hints` to get detailed column schema
    # and defer_table_reflect to reflect schema only during execution
    family = sql_table(
        credentials=ConnectionStringCredentials(
            "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
        ),
        table="family",
        incremental=dlt.sources.incremental(
            "updated",
        ),
        reflection_level="full_with_precision",
        defer_table_reflect=True,
    )
    # columns will be empty here due to defer_table_reflect set to True
    print(family.compute_table_schema())

    # Load all data from another table
    genome = sql_table(
        credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table="genome",
        reflection_level="full_with_precision",
        defer_table_reflect=True,
    )

    # Run the resources together
    info = pipeline.extract([family, genome], write_disposition="merge")
    print(info)
    # Show inferred columns
    print(pipeline.default_schema.to_pretty_yaml())


def select_columns() -> None:
    """Uses table adapter callback to modify list of columns to be selected"""
    pipeline = dlt.pipeline(
        pipeline_name="rfam_database",
        destination="duckdb",
        dataset_name="rfam_data_cols",
        dev_mode=True,
    )

    def table_adapter(table: Table) -> Table:
        print(table.name)
        if table.name == "family":
            # this is SqlAlchemy table. _columns are writable
            # let's drop updated column
            table._columns.remove(table.columns["updated"])  # type: ignore
        return table

    family = sql_table(
        credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table="family",
        chunk_size=10,
        reflection_level="full_with_precision",
        table_adapter_callback=table_adapter,
    )

    # also we do not want the whole table, so we add limit to get just one chunk (10 records)
    pipeline.run(family.add_limit(1))
    # only 10 rows
    print(pipeline.last_trace.last_normalize_info)
    # no "updated" column in "family" table
    print(pipeline.default_schema.to_pretty_yaml())


def select_with_end_value_and_row_order() -> None:
    """Gets data from a table withing a specified range and sorts rows descending"""
    pipeline = dlt.pipeline(
        pipeline_name="rfam_database",
        destination="duckdb",
        dataset_name="rfam_data",
        dev_mode=True,
    )

    # gets data from this range
    start_date = pendulum.now().subtract(years=1)
    end_date = pendulum.now()

    family = sql_table(
        credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table="family",
        incremental=dlt.sources.incremental(  # declares desc row order
            "updated", initial_value=start_date, end_value=end_date, row_order="desc"
        ),
        chunk_size=10,
    )
    # also we do not want the whole table, so we add limit to get just one chunk (10 records)
    pipeline.run(family.add_limit(1))
    # only 10 rows
    print(pipeline.last_trace.last_normalize_info)


def my_sql_via_pyarrow() -> None:
    """Uses pyarrow backend to load tables from mysql"""

    # uncomment line below to get load_id into your data (slows pyarrow loading down)
    # dlt.config["normalize.parquet_normalizer.add_dlt_load_id"] = True

    # Create a pipeline
    pipeline = dlt.pipeline(
        pipeline_name="rfam_cx",
        destination="duckdb",
        dataset_name="rfam_data_arrow_4",
    )

    def _double_as_decimal_adapter(table: sa.Table) -> sa.Table:
        """Return double as double, not decimals, only works if you are using sqlalchemy 2.0"""
        for column in table.columns.values():
            if hasattr(sa, "Double") and isinstance(column.type, sa.Double):
                column.type.asdecimal = False
        return table

    sql_alchemy_source = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam?&binary_prefix=true",
        backend="pyarrow",
        table_adapter_callback=_double_as_decimal_adapter,
    ).with_resources("family", "genome")

    info = pipeline.run(sql_alchemy_source)
    print(info)


def create_unsw_flow() -> None:
    """Uploads UNSW_Flow dataset to postgres via csv stream skipping dlt normalizer.
    You need to download the dataset from https://github.com/rdpahalavan/nids-datasets
    """
    from pyarrow.parquet import ParquetFile

    # from dlt.destinations import postgres

    # use those config to get 3x speedup on parallelism
    # [sources.data_writer]
    # file_max_bytes=3000000
    # buffer_max_items=200000

    # [normalize]
    # workers=3

    data_iter = ParquetFile("UNSW-NB15/Network-Flows/UNSW_Flow.parquet").iter_batches(
        batch_size=128 * 1024
    )

    pipeline = dlt.pipeline(
        pipeline_name="unsw_upload",
        # destination=postgres("postgres://loader:loader@localhost:5432/dlt_data"),
        destination="postgres",
        progress="log",
    )
    pipeline.run(
        data_iter,
        dataset_name="speed_test",
        table_name="unsw_flow_7",
        loader_file_format="csv",
    )


def test_connectorx_speed() -> None:
    """Uses unsw_flow dataset (~2mln rows, 25+ columns) to test connectorx speed"""
    import os

    unsw_table = sql_table(
        "postgresql://loader:loader@localhost:5432/dlt_data",
        "unsw_flow_7",
        "speed_test",
        # this is ignored by connectorx
        chunk_size=100000,
        backend="connectorx",
        # keep source data types
        reflection_level="full_with_precision",
        # just to demonstrate how to setup a separate connection string for connectorx
        backend_kwargs={"conn": "postgresql://loader:loader@localhost:5432/dlt_data"},
    )

    pipeline = dlt.pipeline(
        pipeline_name="unsw_download",
        destination="filesystem",
        # destination=filesystem(os.path.abspath("../_storage/unsw")),
        progress="log",
        dev_mode=True,
    )

    info = pipeline.run(
        unsw_table,
        dataset_name="speed_test",
        table_name="unsw_flow",
        loader_file_format="parquet",
    )
    print(info)


def test_pandas_backend_verbatim_decimals() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rfam_cx",
        destination="duckdb",
        dataset_name="rfam_data_pandas_2",
    )

    def _double_as_decimal_adapter(table: sa.Table) -> sa.Table:
        """Emits decimals instead of floats."""
        for column in table.columns.values():
            if isinstance(column.type, sa.Float):
                column.type.asdecimal = True
        return table

    sql_alchemy_source = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam?&binary_prefix=true",
        backend="pandas",
        table_adapter_callback=_double_as_decimal_adapter,
        chunk_size=100000,
        # set coerce_float to False to represent them as string
        backend_kwargs={"coerce_float": False, "dtype_backend": "numpy_nullable"},
        # preserve full typing info. this will parse
        reflection_level="full_with_precision",
    ).with_resources("family", "genome")

    info = pipeline.run(sql_alchemy_source)
    print(info)


def use_type_adapter() -> None:
    """Example use of type adapter to coerce unknown data types"""
    pipeline = dlt.pipeline(
        pipeline_name="dummy",
        destination="postgres",
        dataset_name="dummy",
    )

    def type_adapter(sql_type: Any) -> Any:
        if isinstance(sql_type, sa.ARRAY):
            return sa.JSON()  # Load arrays as JSON
        return sql_type

    sql_alchemy_source = sql_database(
        "postgresql://loader:loader@localhost:5432/dlt_data",
        backend="pyarrow",
        type_adapter_callback=type_adapter,
        reflection_level="full_with_precision",
    ).with_resources("table_with_array_column")

    info = pipeline.run(sql_alchemy_source)
    print(info)


def specify_columns_to_load() -> None:
    """Run the SQL database source with a subset of table columns loaded"""
    pipeline = dlt.pipeline(
        pipeline_name="dummy",
        destination="duckdb",
    )

    # Columns can be specified per table in env var (json array) or in `.dlt/config.toml`
    os.environ["SOURCES__SQL_DATABASE__FAMILY__INCLUDED_COLUMNS"] = '["rfam_acc", "description"]'

    sql_alchemy_source = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam?&binary_prefix=true",
        backend="pyarrow",
        reflection_level="full_with_precision",
    ).with_resources("family", "genome")

    info = pipeline.run(sql_alchemy_source)
    print(info)


if __name__ == "__main__":
    # Load selected tables with different settings
    # load_select_tables_from_database()

    # load a table and select columns
    # select_columns()

    # load_entire_database()
    # select_with_end_value_and_row_order()

    # Load tables with the standalone table resource
    load_standalone_table_resource()

    # Load all tables from the database.
    # Warning: The sample database is very large
    # load_entire_database()
