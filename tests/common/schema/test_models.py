
import dataclasses

import dlt
from dlt.common.schema.models import table_schema_to_dataclass, _DATACLASS_NAME_TEMPLATE
from tests.common.utils import load_yml_case


def test_table_schema_to_dataclass():
    case = load_yml_case("schemas/eth/ethereum_schema_v5")
    schema = dlt.Schema.from_dict(case, bump_version=True)  # type: ignore[arg-type]
    table_name = "blocks"
    table_schema = schema.tables[table_name]
    dataclass = table_schema_to_dataclass(table_schema)
    assert dataclasses.is_dataclass(dataclass)
    assert dataclass.__name__ == _DATACLASS_NAME_TEMPLATE.format(table_name=table_name)
    assert all(col in dataclass.__dict__ for col in table_schema["columns"])
