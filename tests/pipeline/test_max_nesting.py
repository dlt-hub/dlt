from typing import List

import dlt
import pytest

from dlt.destinations import dummy


example_data = {"one": [{"two": [{"three": [{"four": [{"five": "value"}]}]}]}]}
example_data_with_alternative_tree = {
    "one_alternative": [{"two": [{"three": [{"four": [{"five": "value"}]}]}]}]
}

NESTING_LEVEL_0 = [""]
NESTING_LEVEL_1 = [
    "",
    "__one",
]
NESTING_LEVEL_2 = ["", "__one", "__one__two"]
NESTING_LEVEL_3 = [
    "",
    "__one",
    "__one__two",
    "__one__two__three",
]
NESTING_LEVEL_4 = [
    "",
    "__one",
    "__one__two",
    "__one__two__three",
    "__one__two__three__four",
]


def _table_names_for_base_table(
    _name: str, tables: List[str], alternative_tree: bool = False
) -> List[str]:
    tables = [f"{_name}{table}" for table in tables]
    if alternative_tree:
        tables = [t.replace("__one", "__one_alternative") for t in tables]
    return tables


def _get_pipeline() -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="test_max_table_nesting",
        destination=dummy(timeout=0.1, completed_prob=1),
        dev_mode=True,
    )


@pytest.mark.parametrize(
    "nesting_level_resource,nesting_level_source,expected_table_names",
    (
        (0, 3, NESTING_LEVEL_0),  # resource overrides source
        (1, 3, NESTING_LEVEL_1),
        (2, 3, NESTING_LEVEL_2),
        (3, 3, NESTING_LEVEL_3),
        (4, 3, NESTING_LEVEL_4),
        (5, 3, NESTING_LEVEL_4),
        (6, 3, NESTING_LEVEL_4),
        (None, 3, NESTING_LEVEL_3),
        (None, 4, NESTING_LEVEL_4),
    ),
)
def test_basic_resource_max_nesting(
    nesting_level_resource: int,
    nesting_level_source: int,
    expected_table_names: List[str],
):
    @dlt.resource(max_table_nesting=nesting_level_resource)
    def base_table():
        yield example_data

    @dlt.source(max_table_nesting=nesting_level_source)
    def source():
        return base_table()

    if nesting_level_resource is not None:
        assert "x-normalizer" in base_table._hints
    else:
        assert "x-normalizer" not in base_table._hints

    pipeline = _get_pipeline()
    pipeline.run(source())

    all_table_names = pipeline.default_schema.data_table_names()
    assert set(all_table_names) == set(
        _table_names_for_base_table("base_table", expected_table_names)
    )


def test_multiple_configurations():
    """test different settings on resources and source at the same time"""

    @dlt.resource(max_table_nesting=2)
    def base_table_1():
        yield example_data

    @dlt.resource(max_table_nesting=4)
    def base_table_2():
        yield example_data

    # resource below will inherit from source
    @dlt.resource()
    def base_table_3():
        yield example_data

    @dlt.source(max_table_nesting=3)
    def source():
        return [base_table_1(), base_table_2(), base_table_3()]

    pipeline = _get_pipeline()
    pipeline.run(source())

    all_table_names = pipeline.default_schema.data_table_names()
    assert set(all_table_names) == set(
        _table_names_for_base_table("base_table_1", NESTING_LEVEL_2)
        + _table_names_for_base_table("base_table_2", NESTING_LEVEL_4)
        + _table_names_for_base_table("base_table_3", NESTING_LEVEL_3)
    )


def test_update_table_nesting_level_resource():
    """test if we can update the max_table_nesting level of a resource"""

    @dlt.resource(max_table_nesting=2)
    def base_table_1():
        yield example_data

    pipeline = _get_pipeline()
    pipeline.run(base_table_1())

    all_table_names = pipeline.default_schema.data_table_names()
    assert set(all_table_names) == set(_table_names_for_base_table("base_table_1", NESTING_LEVEL_2))

    base_table_1.max_table_nesting = 3
    assert base_table_1.max_table_nesting == 3
    pipeline.run(base_table_1())

    # NOTE: it will stay the same since the resource column at nesting level 2 is marked as complex
    all_table_names = pipeline.default_schema.data_table_names()
    assert set(all_table_names) == set(_table_names_for_base_table("base_table_1", NESTING_LEVEL_2))

    # loading with alternative data works
    @dlt.resource(max_table_nesting=3)  # type: ignore[no-redef]
    def base_table_1():
        yield example_data_with_alternative_tree

    pipeline.run(base_table_1())
    all_table_names = pipeline.default_schema.data_table_names()
    assert set(all_table_names) == set(
        _table_names_for_base_table("base_table_1", NESTING_LEVEL_2)
    ).union(_table_names_for_base_table("base_table_1", NESTING_LEVEL_3, alternative_tree=True))


def test_update_table_nesting_level_source():
    """test if we can update the max_table_nesting level of a source"""

    @dlt.resource()
    def base_table_1():
        yield example_data

    @dlt.resource()
    def base_table_2():
        yield example_data

    @dlt.resource(max_table_nesting=1)
    def base_table_3():
        yield example_data

    @dlt.source(max_table_nesting=3)
    def source():
        return [base_table_1(), base_table_2(), base_table_3()]

    pipeline = _get_pipeline()
    pipeline.run(source().with_resources("base_table_1"))

    assert set(pipeline.default_schema.data_table_names()) == set(
        _table_names_for_base_table("base_table_1", NESTING_LEVEL_3)
    )

    # change the max_table_nesting level of the source and load another formerly unloaded resource
    source.max_table_nesting = 4  # type: ignore
    pipeline.run(source().with_resources("base_table_1", "base_table_2"))

    # for base_table_1 it will stay the same since it is already loaded
    assert set(pipeline.default_schema.data_table_names()) == set(
        _table_names_for_base_table("base_table_1", NESTING_LEVEL_3)
        + _table_names_for_base_table("base_table_2", NESTING_LEVEL_4)
    )

    # load full source (resource 3 max_table_nesting will be taken from resource)
    pipeline.run(source())

    assert set(pipeline.default_schema.data_table_names()) == set(
        _table_names_for_base_table("base_table_1", NESTING_LEVEL_3)
        + _table_names_for_base_table("base_table_2", NESTING_LEVEL_4)
        + _table_names_for_base_table("base_table_3", NESTING_LEVEL_1)
    )


@pytest.mark.parametrize("nesting_defininition_location", ["resource", "source"])
def test_nesting_levels_reset_after_drop(nesting_defininition_location: str):
    """test if the nesting levels are reset after a drop"""

    @dlt.resource(max_table_nesting=2 if nesting_defininition_location == "resource" else None)
    def base_table_1():
        yield example_data

    @dlt.source(max_table_nesting=2 if nesting_defininition_location == "source" else None)
    def source():
        return base_table_1()

    pipeline = _get_pipeline()
    pipeline.run(source())

    all_table_names = pipeline.default_schema.data_table_names()
    assert set(all_table_names) == set(_table_names_for_base_table("base_table_1", NESTING_LEVEL_2))

    pipeline.drop()
    if nesting_defininition_location == "resource":
        base_table_1.max_table_nesting = 3
    else:
        source.max_table_nesting = 3  # type: ignore[attr-defined]
    pipeline.run(source())

    all_table_names = pipeline.default_schema.data_table_names()
    assert set(all_table_names) == set(_table_names_for_base_table("base_table_1", NESTING_LEVEL_3))
