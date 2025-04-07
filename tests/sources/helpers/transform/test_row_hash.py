import pyarrow as pa
from dlt.sources.helpers.transform import add_row_hash_to_table


def test_add_row_hash_to_table():
    names = ["n_legs", "animals"]
    n_legs = [2, 2, 2]
    animals = ["duck", "duck", "duck"]

    table = pa.Table.from_arrays([n_legs, animals], names=names)

    add_row_hash = add_row_hash_to_table("row_hash")
    table_with_rowhash = add_row_hash(table)
    assert len(table_with_rowhash["row_hash"].unique()) == 1, "Expected identical row hashes"
