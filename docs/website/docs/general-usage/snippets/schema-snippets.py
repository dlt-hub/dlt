import dlt
from dlt.common import Decimal


def test_nested_hints_primary_key() -> None:
    # @@@DLT_SNIPPET_START nested_hints_primary_key
    @dlt.resource(
        primary_key="id",
        write_disposition="merge",
        nested_hints={
            "purchases": dlt.mark.make_nested_hints(
                # column hint is optional - makes sure that customer_id is a first column in the table
                columns=[{"name": "customer_id", "data_type": "bigint"}],
                primary_key=["customer_id", "id"],
                write_disposition="merge",
                references=[
                    {
                        "referenced_table": "customers",
                        "columns": ["customer_id"],
                        "referenced_columns": ["id"],
                    }
                ],
            )
        },
    )
    def customers():
        """Load customer data from a simple python list."""
        yield [
            {
                "id": 1,
                "name": "simon",
                "city": "berlin",
                "purchases": [{"id": 1, "name": "apple", "price": Decimal("1.50")}],
            },
            {
                "id": 2,
                "name": "violet",
                "city": "london",
                "purchases": [{"id": 1, "name": "banana", "price": Decimal("1.70")}],
            },
            {
                "id": 3,
                "name": "tammo",
                "city": "new york",
                "purchases": [{"id": 1, "name": "pear", "price": Decimal("2.50")}],
            },
        ]

    def _pushdown_customer_id(row):
        id_ = row["id"]
        for purchase in row["purchases"]:
            purchase["customer_id"] = id_
        return row

    p = dlt.pipeline(
        pipeline_name="test_nested_hints_primary_key",
        destination="duckdb",
        dataset_name="local",
    )
    p.run(customers().add_map(_pushdown_customer_id))
    # load same data again to prove that merge works
    p.run(customers().add_map(_pushdown_customer_id))
    # check counts
    row_count = p.dataset().row_counts().fetchall()
    assert row_count == [("customers", 3), ("customers__purchases", 3)]
    # @@@DLT_SNIPPET_END nested_hints_primary_key

def test_compound_hints_direct_key_precedence() -> None:
    """Demonstrates that direct key hints take precedence over column level hints."""
    # @@@DLT_SNIPPET_START compound_hints_direct_key_precedence
    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline",
        destination="duckdb",
        dataset_name="my_data",
    )

    @dlt.resource(
        name="my_table",
        primary_key="col_1",
        columns={"col_2": {"primary_key": True}},
    )
    def my_resource():
        yield {"col_1": 1, "col_2": 2}

    pipeline.run(my_resource)
    # @@@DLT_SNIPPET_END compound_hints_direct_key_precedence
   
    # Verify: col_1 should have primary_key (direct key takes precedence)
    assert pipeline.default_schema.tables["my_table"]["columns"]["col_1"].get("primary_key") is True
    assert not pipeline.default_schema.tables["my_table"]["columns"]["col_2"].get("primary_key")

def test_compound_hints_empty_direct_key_precedence() -> None:
    """Demonstrates that direct key hints take precedence over column level hints."""
    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline",
        destination="duckdb",
        dataset_name="my_data",
    )
    # @@@DLT_SNIPPET_START test_compound_hints_empty_direct_key_precedence
    @dlt.resource(
        name="my_table",
        primary_key="",
        columns={"col_2": {"primary_key": True}},
    )
    def my_resource():
        yield {"col_1": 1, "col_2": 2}

    pipeline.run(my_resource)
    # @@@DLT_SNIPPET_END test_compound_hints_empty_direct_key_precedence
   
    # Verify: both should not have primary_key (direct key takes precedence)
    assert not pipeline.default_schema.tables["my_table"]["columns"]["col_1"].get("primary_key")
    assert not pipeline.default_schema.tables["my_table"]["columns"]["col_2"].get("primary_key")

def test_compound_hints_direct_key_precedence_apply_hints() -> None:
    """Demonstrates that direct key hints take precedence over column level hints in apply_hints."""
    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline",
        destination="duckdb",
        dataset_name="my_data",
    )
    # @@@DLT_SNIPPET_START test_compound_hints_direct_key_precedence_apply_hints
    @dlt.resource(
        name="my_table",
    )
    def my_resource():
        yield {"col_1": 1, "col_2": 2}

    my_resource.apply_hints(merge_key="col_1", columns={"col_2": {"merge_key": True}})

    pipeline.run(my_resource)
    # @@@DLT_SNIPPET_END test_compound_hints_direct_key_precedence_apply_hints
   
    # Verify: both should not have primary_key (direct key takes precedence)
    assert pipeline.default_schema.tables["my_table"]["columns"]["col_1"].get("merge_key") is True
    assert not pipeline.default_schema.tables["my_table"]["columns"]["col_2"].get("merge_key")

def test_compound_hints_replace_previous_compound_props() -> None:
    """Demonstrate what new compound prop hints on existing resource replace previous ones."""
    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline1",
        destination="duckdb",
        dataset_name="my_data",
    )
    # @@@DLT_SNIPPET_START test_compound_hints_replace_previous_compound_props
    @dlt.resource(
        name="my_table",
        columns={"col_2": {"partition": True}},
    )
    def my_resource():
        yield {"col_1": 1, "col_2": 2}
    
    pipeline.run(my_resource)

    @dlt.resource(
        name="my_table",
        columns={"col_1": {"partition": True}},
    )
    def my_resource():
        yield {"col_1": 1, "col_2": 2}
    
    pipeline.run(my_resource)
    # @@@DLT_SNIPPET_END test_compound_hints_replace_previous_compound_props

    # Verify: only col_1 should have partition property
    assert pipeline.default_schema.tables["my_table"]["columns"]["col_1"].get("partition") is True
    assert not pipeline.default_schema.tables["my_table"]["columns"]["col_2"].get("partition")

def test_empty_value_key_hints_do_not_replace_previous_hints() -> None:
    """Demonstrate that new empty value key hint hints on existing resource do nothing."""
    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline1",
        destination="duckdb",
        dataset_name="my_data",
    )
    # @@@DLT_SNIPPET_START test_empty_value_key_hints_do_not_replace_previous_hints
    @dlt.resource(
        name="my_table",
        primary_key="col_1",
    )
    def my_resource():
        yield {"col_1": 1, "col_2": 2}

    pipeline.run(my_resource)

    @dlt.resource(
        name="my_table",
        primary_key=[],
    )
    def my_resource():
        yield {"col_1": 1, "col_2": 2}

    pipeline.run(my_resource)
    # @@@DLT_SNIPPET_END test_empty_value_key_hints_do_not_replace_previous_hints

    assert pipeline.default_schema.tables["my_table"]["columns"]["col_1"].get("primary_key") is True

def test_column_level_compound_prop_hints_via_apply_hints_merged() -> None:
    """Demonstrate that column level hints via apply_hints are merged."""
    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline1",
        destination="duckdb",
        dataset_name="my_data",
    )
    # @@@DLT_SNIPPET_START test_column_level_compound_prop_hints_via_apply_hints_merged
    @dlt.resource(
        name="my_table",
        columns={"col_2": {"primary_key": True}},
    )
    def my_resource():
        yield {"col_1": 1, "col_2": 2}

    pipeline.run(my_resource)

    my_resource.apply_hints(columns={"col_1": {"primary_key": True}})

    pipeline.run(my_resource)
    # @@@DLT_SNIPPET_END test_column_level_compound_prop_hints_via_apply_hints_merged

    assert pipeline.default_schema.tables["my_table"]["columns"]["col_1"].get("primary_key") is True
    assert pipeline.default_schema.tables["my_table"]["columns"]["col_2"].get("primary_key") is True

def test_direct_key_hint_via_apply_hints_replaces() -> None:
    """Demonstrate that direct key hints via apply_hints replace."""
    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline1",
        destination="duckdb",
        dataset_name="my_data",
    )
    # @@@DLT_SNIPPET_START test_direct_key_hint_via_apply_hints_replaces
    @dlt.resource(
        name="my_table",
        columns={"col_2": {"primary_key": True}},
    )
    def my_resource():
        yield {"col_1": 1, "col_2": 2}

    pipeline.run(my_resource)

    my_resource.apply_hints(primary_key="col_1")

    pipeline.run(my_resource)
    # @@@DLT_SNIPPET_END test_direct_key_hint_via_apply_hints_replaces

    assert pipeline.default_schema.tables["my_table"]["columns"]["col_1"].get("primary_key") is True
    assert not pipeline.default_schema.tables["my_table"]["columns"]["col_2"].get("primary_key")
