from dlt.destinations import mssql


def test_from_destination_type_mssql_text_types() -> None:
    caps = mssql().capabilities()
    mapper = caps.get_type_mapper()

    assert mapper.from_destination_type("nvarchar", None, None) == {"data_type": "text"}
    assert mapper.from_destination_type("varchar", None, None) == {"data_type": "text"}