"""(EXPERIMENTAL) Helper module to generate data from a `dlt.Schema`

This sample data can be used for testing, demos, and development.

The current goal is not implement deeply realistic synthetic data or
generate massive amounts of sample data.

## Design
This uses a pure functional approach with explicit state being passed around functions.
This approach makes testing easier than having a `DataGenerator` class with stateful attributes.
We can can inspect bugs against specific states and use them as test cases. It also
helps for constrained generation.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import graphlib
import random
import re
import warnings
from collections import defaultdict
from typing import Any, Callable, Optional, Union
from typing_extensions import ParamSpec

import dlt
from dlt.common.data_types import TDataType
from dlt.common.exceptions import TypeErrorWithKnownTypes
from dlt.common.normalizers.utils import generate_dlt_id, generate_dlt_ids
from dlt.common.schema.schema import Schema
from dlt.common.schema import utils as schema_utils
from dlt.common.schema.typing import (
    TColumnSchema,
    TTableSchema,
    TSchemaTables,
    C_DLT_ID,
    C_DLT_LOAD_ID,
)
from mimesis import Field, Fieldset, Schema as MimesisSchema

# Display warning once when module is loaded
warnings.warn(
    "The data generation features are experimental and the API may change in future versions.",
    FutureWarning,
    stacklevel=2,
)

P = ParamSpec("P")


DLT_TO_PY_TYPE_MAP: dict[TDataType, type] = {
    "text": str,
    "double": float,
    "bool": bool,
    "bigint": int,
    "binary": bytes,
    "json": Union[dict, list],
    "wei": decimal.Decimal,
    "decimal": decimal.Decimal,
    "timestamp": datetime.datetime,
    "date": datetime.date,
    "time": datetime.time,
}

# TODO could use fuzzy search with `thefuzz` over column names and descriptions
COLUMN_NAME_PATTERNS: list[tuple[str, str]] = [
    # UUID patterns - check first as they're more specific
    (r".*uuid.*", "cryptographic.uuid"),
    # Email and name patterns
    (r".*email.*", "person.email"),
    (r".*name$|.*_name$|^name$", "person.full_name"),
    (r".*first_?name.*", "person.first_name"),
    (r".*last_?name.*", "person.last_name"),
    # ID patterns - use word boundaries to avoid matching common words like "video", "valid", etc.
    # Matches: id, user_id, product_id, _id, order_id, but NOT: video, idea, valid, holiday
    (r"^id$|.*[_-]id$|^id[_-].*", "cryptographic.uuid"),
    # Internet patterns
    (r".*url.*", "internet.url"),
    (r".*phone.*|.*telephone.*", "person.telephone"),
    # Address patterns
    (r".*address.*", "address.address"),
    (r".*city.*", "address.city"),
    (r".*country.*", "address.country"),
    # Visual patterns
    (r".*color.*", "text.hex_color"),
    # Time patterns
    (r".*_at$|.*created.*|.*updated.*", "datetime.datetime"),
    # Text patterns
    (r".*description.*|.*content.*|.*text.*", "text.text"),
    (r".*title.*", "text.title"),
    # User authentication patterns
    (r".*username.*|.*login.*", "person.username"),
    (r".*password.*", "person.password"),
    # Demographic patterns
    (r".*age.*", "person.age"),
    # Organization patterns
    (r".*company.*|.*organization.*", "company.name"),
]


@dataclasses.dataclass
class GeneratorState:
    """Maintains state for data generation across batches."""

    unique_values: dict[str, set[Any]] = dataclasses.field(default_factory=dict)
    """Maps 'table.column' to set of generated values for uniqueness tracking."""

    generated_ids: dict[str, list[str]] = dataclasses.field(default_factory=dict)
    """Maps table name to list of generated _dlt_id values."""

    generated_records: dict[str, list[dict[str, Any]]] = dataclasses.field(default_factory=dict)
    """Maps table name to list of all generated records."""

    table_record_counts: dict[str, int] = dataclasses.field(
        default_factory=lambda: defaultdict(int)
    )
    """Maps table name to total count of generated records."""

    load_id: str = dataclasses.field(default_factory=generate_dlt_id)
    """Consistent load ID for this generation session."""

    list_idx_counters: dict[tuple[str, str], int] = dataclasses.field(
        default_factory=lambda: defaultdict(int)
    )
    """Maps (table_name, parent_id) to counter for _dlt_list_idx."""


def _mimesis_generator_text(field: Field) -> Callable:
    return field("text.word")


def _mimesis_generator_bigint(field: Field) -> Callable:
    return field("numeric.integer_number", start=-(2**31), end=2**31 - 1)


def _mimesis_generator_double(field: Field) -> Callable:
    return field("numeric.float_number", start=-1000000.0, end=1000000.0, precision=2)


def _mimesis_generator_bool(field: Field) -> Callable:
    return field("development.boolean")


def _mimesis_generator_decimal(field: Field) -> Callable:
    return field("numeric.decimal_number", start=-1000000.0, end=1000000.0)


def _mimesis_generator_wei(field: Field) -> Callable:
    return field("numeric.decimal_number", start=-1000000.0, end=1000000.0)


def _mimesis_generator_timestamp(field: Field) -> Callable:
    return field("datetime.datetime")


def _mimesis_generator_date(field: Field) -> Callable:
    return field("datetime.date")


def _mimesis_generator_time(field: Field) -> Callable:
    return field("datetime.time")


def _mimesis_generator_binary(field: Field) -> Callable:
    return field("cryptographic.token_bytes", entropy=32)


def _mimesis_generator_json(field: Field) -> Callable:
    return lambda: {
        "field1": field("text.word"),
        "field2": field("text.word"),
        "field3": {"start": -(2**8), "end": 2**8 - 1},
    }


def _create_mimesis_generator_from_path(field: Field, mimesis_path: str) -> Callable[[], Any]:
    """Create a mimesis generator from a path string (used for column name pattern matching)."""
    return lambda: field(mimesis_path)


DLT_TO_MIMESIS_GENERATOR: dict[TDataType, Callable[[Field], Callable]] = {
    "text": _mimesis_generator_text,
    "bigint": _mimesis_generator_bigint,
    "double": _mimesis_generator_double,
    "bool": _mimesis_generator_bool,
    "decimal": _mimesis_generator_decimal,
    "wei": _mimesis_generator_wei,
    "timestamp": _mimesis_generator_timestamp,
    "date": _mimesis_generator_date,
    "time": _mimesis_generator_time,
    "binary": _mimesis_generator_binary,
    "json": _mimesis_generator_json,
}

def _select_mimesis_generator(
    column_name: str, column_schema: TColumnSchema, field: Field
) -> Callable[[], Any]:
    """Select appropriate generator based on column schema and name (pure function)."""
    data_type: TDataType = column_schema.get("data_type", "text")

    dlt_type_generator_func = DLT_TO_MIMESIS_GENERATOR.get(data_type, _mimesis_generator_text)
    dlt_type_value_generator = dlt_type_generator_func(field)

    # for pattern, regex_mimesis_path in COLUMN_NAME_PATTERNS:
    #     if re.match(pattern, column_name, re.IGNORECASE):
    #         regex_value_generator = _create_mimesis_generator_from_path(field, regex_mimesis_path)
    #         # a bit ugly; generate a value for both mimesis generators and ensure they
    #         # return the same data type.
    #         # This is to avoid regex overriding the `dlt` type defined on the schema
    #         if type(regex_value_generator()) == type(dlt_type_value_generator()):
    #             return regex_value_generator

    return dlt_type_value_generator


def _generate_unique_value(
    generator_func: Callable[[], Any],
    state: GeneratorState,
    state_key: str,
    max_retries: int,
) -> Any:
    """Generate a unique value, retrying if necessary"""
    if state_key not in state.unique_values:
        state.unique_values[state_key] = set()

    for _ in range(max_retries):
        value = generator_func()
        if value not in state.unique_values[state_key]:
            state.unique_values[state_key].add(value)
            return value

    # log
    base_value = generator_func()
    counter = len(state.unique_values[state_key])
    unique_value = f"{base_value}_{counter}"
    state.unique_values[state_key].add(unique_value)
    return unique_value


def _generate_dlt_column_value(
    column_name: str,
    state: GeneratorState,
    context: dict[str, Any],
) -> Any:
    """Generate special dlt column values"""
    if column_name == C_DLT_ID:
        value = generate_dlt_id()
    elif column_name == C_DLT_LOAD_ID:
        value = state.load_id
    elif column_name == "_dlt_parent_id":
        value = context["_dlt_parent_id"] if "_dlt_parent_id" in context else None
    elif column_name == "_dlt_root_id":
        value = context["_dlt_root_id"] if "_dlt_root_id" in context else None
    elif column_name == "_dlt_list_idx":
        value = context["_dlt_list_idx"] if "_dlt_list_idx" in context else 0
    else:
        value = None

    return value


def _should_generate_null(
    is_nullable: bool,
    is_unique: bool,
    is_primary_key: bool,
    null_probability: float,
) -> bool:
    """Determine if a null value should be generated."""
    if is_primary_key or is_unique:
        value = False
    elif is_nullable and random.random() < null_probability:
        value = True
    else:
        value = False
    return value


def generate_column_value(
    *,
    state: GeneratorState,
    column_name: str,
    column_schema: TColumnSchema,
    generator_func: Callable[[], Any],
    context: Optional[dict[str, Any]],
    unique_key_prefix: Optional[str],
    null_probability: float,
) -> Any:
    """Generate a single column value with constraints applied."""
    context = {} if context is None else context
    dlt_value = _generate_dlt_column_value(column_name, state, context)
    if dlt_value is not None or column_name.startswith("_dlt_"):
        return dlt_value

    is_nullable = column_schema.get("nullable", True)
    is_unique = column_schema.get("unique", False)
    is_primary_key = column_schema.get("primary_key", False)

    if _should_generate_null(is_nullable, is_unique, is_primary_key, null_probability):
        return None

    if is_primary_key or is_unique:
        unique_key = f"{unique_key_prefix}.{column_name}" if unique_key_prefix else column_name
        return _generate_unique_value(generator_func, state, unique_key, max_retries=100)

    return generator_func()


def _generate_single_record(
    *,
    table_schema: TTableSchema,
    field: Field,
    state: GeneratorState,
    context: Optional[dict[str, Any]],
    unique_key_prefix: str,
    include_dlt_columns: bool,
    dlt_prefix: str,
    null_probability: float,
) -> dict[str, Any]:
    """Generate a single record (functional core logic).

    Returns:
        tuple of (record dict, dlt_id value or None)
    """
    record = {}
    for column_name, col_schema in table_schema.get("columns", {}).items():
        value = generate_column_value(
            column_name=column_name,
            column_schema=col_schema,
            generator_func=_select_mimesis_generator(column_name, col_schema, field),
            state=state,
            context=context,
            unique_key_prefix=unique_key_prefix,
            null_probability=null_probability,
        )
        if include_dlt_columns or not column_name.startswith(dlt_prefix):
            record[column_name] = value

    return record


def _update_table_state(
    state: GeneratorState,
    table_name: str,
    records: list[dict[str, Any]],
    dlt_ids: list[str],
) -> None:
    """Update state after generating records."""
    if dlt_ids:
        if table_name not in state.generated_ids:
            state.generated_ids[table_name] = []
        state.generated_ids[table_name].extend(dlt_ids)

    if table_name not in state.generated_records:
        state.generated_records[table_name] = []
    state.generated_records[table_name].extend(records)

    state.table_record_counts[table_name] += len(records)


def generate_table_batch(
    *,
    n_records: int,
    table_schema: TTableSchema,
    table_name: str,
    field: Field,
    state: GeneratorState,
    parent_context: Optional[dict[str, Any]],
    include_dlt_columns: bool,
    dlt_prefix: str,
    null_probability: float,
) -> list[dict[str, Any]]:
    """Generate multiple records for a table.

    Returns:
        list of generated records
    """
    records = []
    dlt_ids = []

    for i in range(n_records):
        context = {}
        if parent_context:
            context.update(parent_context)

        record = _generate_single_record(
            table_schema=table_schema,
            field=field,
            state=state,
            context=context,
            unique_key_prefix=table_name,
            include_dlt_columns=include_dlt_columns,
            dlt_prefix=dlt_prefix,
            null_probability=null_probability,
        )
        records.append(record)

        if record.get("_dlt_id") is not None:
            dlt_ids.append(record["_dlt_id"])

    _update_table_state(state, table_name, records, dlt_ids)
    return records


def _build_dependency_graph(schema: dlt.Schema) -> dict[str, set[str]]:
    """Build dependency graph from schema references and parent fields."""
    graph = {}

    for table_name, table_schema in schema.tables.items():
        dependencies = set()

        # Add parent table dependency
        if "parent" in table_schema:
            parent = table_schema["parent"]
            if isinstance(parent, str):
                dependencies.add(parent)

        # Add foreign key dependencies from references
        # Note: We skip special dlt references (_dlt_parent, _dlt_root, _dlt_load)
        for ref in table_schema.get("x-normalizer", {}).get("references", []):
            if ref.get("referenced_table") and ref.get("referenced_table") != table_name:
                dependencies.add(ref["referenced_table"])

        graph[table_name] = dependencies

    return graph


def _get_root_table(tables: TSchemaTables, table_name: str) -> str:
    """Finds root (without parent) of a `table_name` following the nested references (row_key - parent_key)."""
    table = tables[table_name]
    if schema_utils.is_nested_table(table):
        return _get_root_table(tables, table.get("parent"))
    return table_name


def _get_parent_table_name(table_schema: TTableSchema) -> Optional[str]:
    """Extract parent table name from table schema (pure function)."""
    parent = table_schema.get("parent")
    return parent if isinstance(parent, str) else None


def _compute_dependency_order(schema: Schema) -> list[str]:
    """Compute topological order of tables based on dependencies (pure function)."""
    graph = _build_dependency_graph(schema)
    try:
        ts = graphlib.TopologicalSorter(graph)
        return list(ts.static_order())
    except graphlib.CycleError:
        raise ValueError("Circular dependency detected in schema")


def _generate_nested_record(
    *,
    state: GeneratorState,
    table_schema: TTableSchema,
    table_name: str,
    field: Field,
    parent_ids: list[str],
    root_ids: list[str],
    include_dlt_columns: bool,
    dlt_prefix: str,
    null_probability: float,
) -> list[dict]:
    """Generate a single nested record with proper parent/root context (functional core logic)."""
    parent_id = random.choice(parent_ids)
    root_id = random.choice(root_ids) if root_ids else parent_id
    list_idx_key = (table_name, parent_id)
    list_idx = state.list_idx_counters[list_idx_key]
    context = {"_dlt_parent_id": parent_id, "_dlt_root_id": root_id, "_dlt_list_idx": list_idx}

    record_batch = generate_table_batch(
        n_records=1,
        table_schema=table_schema,
        table_name=table_name,
        field=field,
        state=state,
        parent_context=context,
        include_dlt_columns=include_dlt_columns,
        dlt_prefix=dlt_prefix,
        null_probability=null_probability,
    )
    state.list_idx_counters[list_idx_key] += 1
    return record_batch


def _ensure_parent_data_exists(
    *,
    parent_table_name: str,
    state: GeneratorState,
    auto_resolve: bool,
    generate_func: Callable[[str, ...], Any],
    n_records: int,
) -> None:
    """Ensure parent table has generated data (orchestration logic)."""
    if not state.generated_ids.get(parent_table_name):
        if auto_resolve:
            generate_func(parent_table_name, n_records=max(10, n_records // 2))
        else:
            raise ValueError(
                f"Parent table '{parent_table_name}' must be generated before nested table. "
                "Set auto_resolve_references=True or generate parent first."
            )


def _resolve_dependencies(
    *,
    table_name: str,
    schema: Schema,
    state: GeneratorState,
    auto_resolve: bool,
    generate_func: Callable[[str, int], Any],
    n_records: int,
) -> None:
    """Resolve table dependencies by generating required parent data (orchestration logic)."""
    if auto_resolve:
        dependencies = _build_dependency_graph(schema).get(table_name, set())
        for dep in dependencies:
            if dep not in state.generated_ids or not state.generated_ids[dep]:
                generate_func(dep, n_records=max(10, n_records // 2))


def _reset_table_state(state: GeneratorState, table_name: str) -> None:
    """Reset state for a specific table (explicit state mutation)."""
    if table_name in state.generated_ids:
        del state.generated_ids[table_name]

    if table_name in state.generated_records:
        del state.generated_records[table_name]

    keys_to_remove = [key for key in state.unique_values if key.startswith(f"{table_name}.")]
    for key in keys_to_remove:
        del state.unique_values[key]

    if table_name in state.table_record_counts:
        del state.table_record_counts[table_name]

    keys_to_remove = [key for key in state.list_idx_counters if key[0] == table_name]
    for key in keys_to_remove:
        del state.list_idx_counters[key]


def _get_table_state_info(state: GeneratorState, table_name: str) -> dict[str, Any]:
    """Get state information for a table (pure function)."""
    return {
        "record_count": state.table_record_counts.get(table_name, 0),
        "generated_ids_count": len(state.generated_ids.get(table_name, [])),
    }


def generate_table(
    *,
    table_name: str,
    n_records: int,
    schema: Schema,
    field: Field,
    state: GeneratorState,
    auto_resolve: bool,
    include_dlt_columns: bool,
    null_probability: float,
    generate_func: Callable[[str, int], Any],
) -> list[dict[str, Any]]:
    """Generate data for a single table (main table generation orchestration).

    Args:
        table_name: Name of the table to generate data for
        n_records: Number of records to generate
        schema: The dlt Schema
        field: Mimesis field generator
        state: Current generation state
        auto_resolve: Whether to auto-generate parent tables
        include_dlt_columns: Whether to include dlt columns in output
        null_probability: Probability of generating NULL values
        generate_func: Function to call for generating dependent tables

    Returns:
        list of generated records
    """
    if table_name not in schema.tables:
        raise ValueError(f"Table `{table_name}` not found in schema")

    table_schema = schema.tables[table_name]

    if schema_utils.is_nested_table(table_schema):
        # Generate nested table data
        parent_table_name = _get_parent_table_name(table_schema)
        if not parent_table_name:
            raise ValueError(f"Cannot determine parent table for `{table_name}`")

        _ensure_parent_data_exists(
            parent_table_name=parent_table_name,
            state=state,
            auto_resolve=auto_resolve,
            generate_func=generate_func,
            n_records=n_records,
        )

        records = []
        for _ in range(n_records):
            record_batch = _generate_nested_record(
                state=state,
                table_schema=table_schema,
                table_name=table_name,
                field=field,
                parent_ids=state.generated_ids[parent_table_name],
                root_ids=state.generated_ids.get(
                    _get_root_table(schema.tables, table_name),
                    state.generated_ids[parent_table_name],
                ),
                include_dlt_columns=include_dlt_columns,
                dlt_prefix=schema._dlt_tables_prefix,
                null_probability=null_probability,
            )
            records.extend(record_batch)

        return records
    else:
        # Generate root table data
        _resolve_dependencies(
            table_name=table_name,
            schema=schema,
            state=state,
            auto_resolve=auto_resolve,
            generate_func=generate_func,
            n_records=n_records,
        )

        return generate_table_batch(
            n_records=n_records,
            table_schema=table_schema,
            table_name=table_name,
            field=field,
            state=state,
            parent_context=None,
            include_dlt_columns=include_dlt_columns,
            dlt_prefix=schema._dlt_tables_prefix,
            null_probability=null_probability,
        )


def generate_all_tables(
    *,
    schema: Schema,
    field: Field,
    state: GeneratorState,
    n_records: Union[int, dict[str, int]],
    auto_resolve: bool,
    include_dlt_columns: bool,
    null_probability: float,
    generate_func: Callable[[str, int], Any],
) -> dict[str, list[dict[str, Any]]]:
    """Generate data for all tables in dependency order.

    Args:
        schema: The dlt Schema
        field: Mimesis field generator
        state: Current generation state
        n_records: Number of records to generate per table (int or dict mapping table names)
        auto_resolve: Whether to auto-generate parent tables
        include_dlt_columns: Whether to include dlt columns in output
        null_probability: Probability of generating NULL values
        generate_func: Function to call for generating dependent tables

    Returns:
        dict mapping table names to lists of generated records
    """
    result = {}
    for table_name in _compute_dependency_order(schema):
        if isinstance(n_records, dict):
            n = n_records.get(table_name, 10)
        else:
            n = n_records

        result[table_name] = generate_func(table_name, n_records=n)

    return result


class DatasetGenerator:
    """Convenience wrapper for stateful data generation."""

    def __init__(
        self,
        schema: dlt.Schema,
        seed: Optional[int] = None,
        locale: str = "en",
        auto_resolve_references: bool = True,
        null_probability: float = 0.3,
        max_unique_retries: int = 100,
        include_dlt_columns: bool = True,
    ):
        """
        Initialize the data generator.

        Args:
            schema: The dlt Schema to generate data for
            seed: Random seed for reproducibility
            locale: Mimesis locale (default: "en")
            auto_resolve_references: Automatically generate parent tables (default: True)
            null_probability: Probability of NULL for nullable columns (default: 0.3)
            max_unique_retries: Max attempts to generate unique value (default: 100)
            include_dlt_columns: Include internal dlt columns in generated data (default: True)
        """
        self.schema = schema
        self.seed = seed
        self.locale = locale
        self.auto_resolve_references = auto_resolve_references
        self.null_probability = null_probability
        self.max_unique_retries = max_unique_retries
        self.include_dlt_columns = include_dlt_columns

        if seed is not None:
            random.seed(seed)

        self.field = Field(locale, seed=seed)
        self.state = GeneratorState()

    def generate(
        self,
        table_name: str,
        *,
        n_records: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Generate data for a single table.

        Args:
            table_name: Name of the table to generate data for
            n_records: Number of records to generate

        Returns:
            list of generated records
        """
        return generate_table(
            table_name=table_name,
            n_records=n_records,
            schema=self.schema,
            field=self.field,
            state=self.state,
            auto_resolve=self.auto_resolve_references,
            include_dlt_columns=self.include_dlt_columns,
            null_probability=self.null_probability,
            generate_func=self.generate,
        )

    def generate_all(
        self, n_records: Union[int, dict[str, int]] = 10
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Generate data for all tables in dependency order.

        Args:
            n_records: Number of records to generate per table (int or dict mapping table names)

        Returns:
            dict mapping table names to lists of generated records
        """
        return generate_all_tables(
            schema=self.schema,
            field=self.field,
            state=self.state,
            n_records=n_records,
            auto_resolve=self.auto_resolve_references,
            include_dlt_columns=self.include_dlt_columns,
            null_probability=self.null_probability,
            generate_func=self.generate,
        )

    def reset(self) -> None:
        """Reset all state."""
        self.state = GeneratorState()
        if self.seed is not None:
            random.seed(self.seed)

    def reset_table(self, table_name: str) -> None:
        """Reset state for a specific table."""
        _reset_table_state(self.state, table_name)

    def get_table_state(self, table_name: str) -> dict[str, Any]:
        """Get state information for a table."""
        return _get_table_state_info(self.state, table_name)


def generate_data(
    obj: Union[dlt.Schema, dlt.Dataset, dlt.Pipeline],
    table_name: str,
    *,
    n_records: int = 5,
    include_dlt_columns: bool = True,
) -> list[dict[str, Any]]:
    """
    Generate sample data for a table.

    Args:
        obj: dlt.Schema, dlt.Dataset, or dlt.Pipeline
        table_name: Name of the table to generate data for
        n_records: Number of records to generate (default: 5)
        include_dlt_columns: Include internal dlt columns in generated data (default: True)

    Returns:
        list of generated records as dicts
    """
    if isinstance(obj, dlt.Schema):
        schema = obj
    elif isinstance(obj, dlt.Dataset):
        schema = obj.schema
    elif isinstance(obj, dlt.Pipeline):
        schema = obj.default_schema
    else:
        raise TypeErrorWithKnownTypes("obj", obj, ["dlt.Schema", "dlt.Dataset", "dlt.Pipeline"])

    generator = DatasetGenerator(schema, include_dlt_columns=include_dlt_columns)
    return generator.generate(table_name, n_records=n_records)



def _create_mimesis_schema(dlt_table_schema: TTableSchema, field: Field) -> MimesisSchema:
    _schema = lambda: {
        column_name: _select_mimesis_generator(column_name, column, field)
        for column_name, column in dlt_table_schema["columns"].items()
    }
    return MimesisSchema(_schema)



if __name__ == "__main__":
    import dlt
    from mimesis import Field, Fieldset, Schema
    from mimesis.enums import Gender, TimestampFormat

    field = Field()
    fieldset = Fieldset()

    # schema_definition = lambda: {
    #     "pk": field("increment"),
    #     "uid": field("uuid"),
    #     "name": field("text.word"),
    #     "version": field("version"),
    #     "timestamp": field("timestamp", fmt=TimestampFormat.POSIX),
    #     "owner": {
    #         "email": field("person.email", domains=["mimesis.name"]),
    #         "creator": field("full_name", gender=Gender.FEMALE),
    #     },
    #     "apiKeys": fieldset("token_hex", key=lambda s: s[:16], i=3),
    # }

    # schema = Schema(schema=schema_definition, iterations=3)
    schema = dlt.Schema("test")
    schema.update_table(
        {
            "name": "users",
            "columns": {
                "_dlt_id": {"name": "_dlt_id", "data_type": "text", "nullable": False},
                "_dlt_load_id": {"name": "_dlt_load_id", "data_type": "text", "nullable": False},
                "id": {
                    "name": "id",
                    "data_type": "bigint",
                    "nullable": False,
                    "primary_key": True,
                    "unique": True,
                },
                "email": {"name": "email", "data_type": "text", "nullable": False, "unique": True},
                "name": {"name": "name", "data_type": "text", "nullable": True},
                "age": {"name": "age", "data_type": "bigint", "nullable": True},
                "active": {"name": "active", "data_type": "bool", "nullable": True},
            },
        }
    )

    mschema = _create_mimesis_schema(schema.tables["users"], field)
