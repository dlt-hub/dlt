import dlt

from dlt.destinations.weaviate.naming import NamingConvention

from tests.common.utils import load_yml_case


@dlt.source
def small():
    return dlt.resource([1,2,3], name="table")


def test_table_name_normalization() -> None:
    n = NamingConvention()
    assert n.normalize_table_identifier("FlatSpace") == "FlatSpace"
    assert n.normalize_table_identifier("Flat__Space") == "FlatSpace"
    assert n.normalize_table_identifier("_Flat__Space_") == "FlatSpacex"
    assert n.normalize_table_identifier("_Flat__Sp💡ace_") == "FlatSpAcex"
    assert n.normalize_table_identifier(" Flat Sp!ace ") == "FlatSpAce"


def test_property_normalization() -> None:
    n = NamingConvention()
    assert n.normalize_identifier("camelCase") == "camelCase"
    assert n.normalize_identifier("_camelCase") == "_camelCase"
    assert n.normalize_identifier("_snake_case") == "_snake_case"
    assert n.normalize_identifier("_snake_case_") == "_snake_casex"
    assert n.normalize_identifier("Snake---🛑case_") == "Snake_casex"
    # dashes are compacted
    assert n.normalize_identifier("Snake-______c__ase_") == "Snake_c_asex"
    # but not the leading


def test_reserved_property_names() -> None:
    n = NamingConvention()
    assert n.normalize_identifier("id") == "__id"
    assert n.normalize_identifier("_id") == "___id"
    assert n.normalize_identifier("__id") == "__id"
    assert n.normalize_identifier("___id") == "___id"
    assert n.normalize_identifier("_additional") == "__additional"
    assert n.normalize_identifier("__additional") == "__additional"


# def test_schema_naming() -> None:
#     pipeline = dlt.pipeline(destination="weaviate", dataset_name="FlatSpace")
#     print(pipeline.dataset_name)

#     schema = dlt.Schema("schema")
#     print(schema.name)
#     print(schema.naming)

#     s = small()
#     s = small()
#     print(s._schema.name)
#     print(s._schema.naming)

#     schema_2 = dlt.Schema("Bubababa")
#     print(schema_2.name)
#     print(schema_2.naming)

#     eth_v6 = load_yml_case("schemas/eth/ethereum_schema_v6")
#     eth_v6_schema = dlt.Schema.from_dict(eth_v6)

#     pipeline.extract(s, schema=eth_v6_schema)

#     print(eth_v6_schema.data_tables())
#     print(eth_v6_schema.dlt_tables())


# def test_x_schema_naming_normalize() -> None:
#     pipeline = dlt.pipeline(dataset_name="flat_space")
#     print(pipeline.dataset_name)

#     s = small()
#     eth_v6 = load_yml_case("schemas/eth/ethereum_schema_v6")
#     eth_v6_schema = dlt.Schema.from_dict(eth_v6)

#     pipeline.extract(s, schema=eth_v6_schema)
#     print(eth_v6_schema.tables.keys())
#     default_schema = pipeline.default_schema
#     print(default_schema.name)
#     print(eth_v6_schema.tables.keys())

#     pipeline.run(s, destination="weaviate")
#     print(default_schema.tables.keys())
#     print(pipeline.default_schema.tables.keys())
