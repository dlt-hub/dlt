from typing import Any, Callable, Dict, Optional, Sequence

from dlt import version
from dlt.common.exceptions import MissingDependencyException

try:
    from pyiceberg.transforms import (
        Transform,
        IdentityTransform,
        YearTransform,
        MonthTransform,
        DayTransform,
        HourTransform,
        BucketTransform,
        TruncateTransform,
        S,
    )
    from pyiceberg.partitioning import (
        PartitionSpec as IcebergPartitionSpec,
        PartitionField,
        PARTITION_FIELD_ID_START,
    )
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.io.pyarrow import pyarrow_to_schema
    from pyiceberg.table.name_mapping import NameMapping, MappedField
except ImportError:
    raise MissingDependencyException(
        "dlt iceberg partition spec",
        [f"{version.DLT_PKG_NAME}[pyiceberg]"],
        "Install `pyiceberg` for dlt iceberg partition spec utilities to work",
    )

from dlt.common.libs.pyarrow import pyarrow as pa
from dlt.destinations.impl.filesystem.iceberg_adapter import PartitionSpec


_TRANSFORM_LOOKUP: Dict[str, Callable[[Optional[int]], Transform[S, Any]]] = {
    "identity": lambda _: IdentityTransform(),
    "year": lambda _: YearTransform(),
    "month": lambda _: MonthTransform(),
    "day": lambda _: DayTransform(),
    "hour": lambda _: HourTransform(),
    "bucket": lambda n: BucketTransform(n),
    "truncate": lambda w: TruncateTransform(w),
}


def get_partition_transform(spec: PartitionSpec) -> Transform[S, Any]:
    """Get the PyIceberg Transform object for a partition spec.

    Args:
        spec: The PartitionSpec to get the transform for

    Returns:
        A PyIceberg Transform object

    Raises:
        ValueError: If the transform is not recognized
    """
    try:
        factory = _TRANSFORM_LOOKUP[spec.transform]
    except KeyError as exc:
        raise ValueError(f"Unknown partition transformation type: {spec.transform}") from exc
    return factory(spec.param_value)


def build_iceberg_partition_spec(
    arrow_schema: pa.Schema,
    spec_list: Sequence[PartitionSpec],
) -> tuple[IcebergPartitionSpec, IcebergSchema]:
    """
    Turn a dlt PartitionSpec list into a PyIceberg PartitionSpec.
    Returns the PartitionSpec and the IcebergSchema derived from the Arrow schema.
    """
    name_mapping = _build_name_mapping(arrow_schema)
    iceberg_schema: IcebergSchema = pyarrow_to_schema(arrow_schema, name_mapping)

    fields: list[PartitionField] = []
    for pos, spec in enumerate(spec_list):
        iceberg_field = iceberg_schema.find_field(spec.source_column)

        fields.append(
            PartitionField(
                field_id=PARTITION_FIELD_ID_START + pos,
                source_id=iceberg_field.field_id,
                transform=get_partition_transform(spec),
                name=_default_field_name(spec),
            )
        )

    return IcebergPartitionSpec(*fields), iceberg_schema


def _build_name_mapping(arrow_schema: pa.Schema) -> NameMapping:
    """Builds a name mapping with ids for every field of the schema, nested ones included.

    The mapping is applied to the schema converted from Arrow, which fails on any field
    without a mapped id. Struct, list and map members are mapped under the names Iceberg
    uses for them (`element`, `key` and `value` for the last two).
    """
    next_id = 0

    def map_field(name: str, arrow_type: pa.DataType) -> MappedField:
        nonlocal next_id
        next_id += 1
        field_id = next_id
        children: list[MappedField] = []
        if pa.types.is_struct(arrow_type):
            for child in arrow_type:
                children.append(map_field(child.name, child.type))
        elif (
            pa.types.is_list(arrow_type)
            or pa.types.is_large_list(arrow_type)
            or pa.types.is_fixed_size_list(arrow_type)
        ):
            children.append(map_field("element", arrow_type.value_type))
        elif pa.types.is_map(arrow_type):
            children.append(map_field("key", arrow_type.key_type))
            children.append(map_field("value", arrow_type.item_type))
        return MappedField(field_id=field_id, names=[name], fields=children)  # type: ignore[call-arg]

    return NameMapping([map_field(field.name, field.type) for field in arrow_schema])


def _default_field_name(spec: PartitionSpec) -> str:
    """
    Replicate Iceberg's automatic partition-field naming by delegating to the private
    _PartitionNameGenerator. Falls back to the user-supplied `partition_field` if present.
    """
    from pyiceberg.partitioning import _PartitionNameGenerator

    name_generator = _PartitionNameGenerator()

    if spec.partition_field:  # user-supplied `partition_field`
        return spec.partition_field

    # name generator requires field_id and source_id, but does not use them
    dummy_field_id = 0
    dummy_source_id = 0

    if spec.transform == "bucket":
        # bucket / truncate need the numeric parameter
        return name_generator.bucket(
            dummy_field_id, spec.source_column, dummy_source_id, spec.param_value
        )
    if spec.transform == "truncate":
        return name_generator.truncate(
            dummy_field_id, spec.source_column, dummy_source_id, spec.param_value
        )

    # identity, year, month, day, hour – all have the same signature
    method = getattr(name_generator, spec.transform)

    return method(dummy_field_id, spec.source_column, dummy_source_id)  # type: ignore[no-any-return]
