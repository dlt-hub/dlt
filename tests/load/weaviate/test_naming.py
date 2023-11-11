import dlt, pytest

from dlt.destinations.impl.weaviate.naming import NamingConvention
from dlt.destinations.impl.weaviate.ci_naming import NamingConvention as CINamingConvention

from tests.common.utils import load_yml_case

@dlt.source
def small():
    return dlt.resource([1,2,3], name="table")


@pytest.mark.parametrize("n", [NamingConvention(), CINamingConvention()], ids=["naming", "ci_naming"])
def test_table_name_normalization(n: NamingConvention) -> None:
    assert n.normalize_table_identifier("FlatSpace") == "FlatSpace"
    assert n.normalize_table_identifier("a_snake_case_name") == "ASnakeCaseName"
    assert n.normalize_table_identifier("_a_snake_case_name") == "ASnakeCaseName"
    assert n.normalize_table_identifier("_a_1snake_case_name") == "A1snakeCaseName"
    assert n.normalize_table_identifier("1_a_1snake_case_name") == "C1A1snakeCaseName"
    assert n.normalize_table_identifier("Flat__Space") == "Flat_Space"
    assert n.normalize_table_identifier("_Flat__Space_") == "Flat_Spacex"
    assert n.normalize_table_identifier("_Flat__Sp💡ace_") == "Flat_SpAcex"
    assert n.normalize_table_identifier(" Flat Sp!ace ") == "Flat_SpAce"
    assert n.normalize_table_identifier("1") == "C1"
    assert n.normalize_table_identifier("______") == "Xxxxxx"
    assert n.normalize_table_identifier("1______1") == "C11"


def test_property_normalization() -> None:
    n = NamingConvention()
    assert n.normalize_identifier("camelCase") == "camelCase"
    assert n.normalize_identifier("_camelCase") == "_camelCase"
    assert n.normalize_identifier("_snake_case") == "_snake_case"
    assert n.normalize_identifier("_snake_case_") == "_snake_casex"
    assert n.normalize_identifier("Snake---🛑case_") == "snake_casex"
    assert n.normalize_identifier("--🛑Snake---🛑case_") == "___Snake_casex"
    # dashes are compacted
    assert n.normalize_identifier("Snake-______c__ase_") == "snake_c_asex"
    assert n.normalize_identifier("Snake-______c__ase_") == "snake_c_asex"
    # but not the leading
    assert n.normalize_identifier("-______Snake-______c__ase_") == "_______Snake_c_asex"
    # starting digit
    assert n.normalize_identifier("281782918739821") == "p_281782918739821"


def test_property_normalization_ci() -> None:
    n = CINamingConvention()
    assert n.normalize_identifier("camelCase") == "camelcase"
    assert n.normalize_identifier("_camelCase") == "_camelcase"
    assert n.normalize_identifier("_snake_case") == "_snake_case"
    assert n.normalize_identifier("_snake_case_") == "_snake_casex"
    assert n.normalize_identifier("Snake---🛑case_") == "snake_casex"
    assert n.normalize_identifier("--🛑Snake---🛑case_") == "___snake_casex"
    # dashes are compacted
    assert n.normalize_identifier("Snake-______c__ase_") == "snake_c_asex"
    assert n.normalize_identifier("Snake-______c__ase_") == "snake_c_asex"
    # but not the leading
    assert n.normalize_identifier("-______Snake-______c__ase_") == "_______snake_c_asex"
    # starting digit
    assert n.normalize_identifier("281782918739821") == "p_281782918739821"


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
