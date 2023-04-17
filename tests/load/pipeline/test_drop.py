from typing import Any, Iterator, Dict, Any
import secrets

import dlt
from dlt.extract.source import DltResource
from dlt.common.utils import uniq_id
from dlt.pipeline import helpers, state_sync
from dlt.common.pipeline import _resource_state
from tests.load.pipeline.utils import drop_pipeline


def test_drop_command():
    # Temporary test case
    @dlt.resource
    def a(a=dlt.sources.incremental('a')) -> Any:
        yield dict(a=1, b=2, c=3)
        yield dict(a=4, b=23, c=24)

    @dlt.resource
    def b(asd=dlt.sources.incremental('asd')) -> Any:
        yield dict(
            asd=2323, qe=555, items=[
                dict(m=1, n=2),
                dict(m=3, n=4)
            ]
        )

    @dlt.resource
    def c() -> Any:
        yield dict(asdasd=2424, qe=111, items=[
            dict(k=2, r=2, labels=[dict(name='abc'), dict(name='www')])
        ])

    @dlt.source
    def some_source():
        return [a(), b(), c()]

    pipeline = dlt.pipeline(pipeline_name='drop_test', destination='postgres', dataset_name='drop_data_d')
    with pipeline.sql_client() as client:
        dest_state = state_sync.load_state_from_destination(pipeline.pipeline_name, client)
    # breakpoint()
    attached = dlt.attach(pipeline.pipeline_name, pipeline.pipelines_dir)
    r = helpers.drop(attached, ['a', 'c'])
    source = some_source()
    pipeline.run(source)

    attached = dlt.attach(pipeline.pipeline_name, pipeline.pipelines_dir)
    r = helpers.drop(attached, ['a', 'c'])

    # Verify schema
    attached2 = dlt.attach(pipeline.pipeline_name, pipeline.pipelines_dir)
    assert 'a' not in attached2.default_schema.tables
    assert 'c__items' not in attached2.default_schema.tables


@dlt.source(section='droppable', name='droppable')
def droppable_source():
    @dlt.resource
    def droppable_a(a=dlt.sources.incremental('a', initial_value=0)) -> Iterator[Dict[str, Any]]:
        yield dict(a=1, b=2, c=3)
        yield dict(a=4, b=23, c=24)


    @dlt.resource
    def droppable_b() -> Iterator[Dict[str, Any]]:
        # Child table
        yield dict(asd=2323, qe=555, items=[dict(m=1, n=2), dict(m=3, n=4)])


    @dlt.resource
    def droppable_c() -> Iterator[Dict[str, Any]]:
        dlt.state()
        # Grandchild table
        yield dict(asdasd=2424, qe=111, items=[
            dict(k=2, r=2, labels=[dict(name='abc'), dict(name='www')])
        ])
    return [droppable_a(), droppable_b(), droppable_c()]


def test_drop_command2(drop_pipeline):
    source = droppable_source()
    pipeline = dlt.pipeline(pipeline_name='drop_test_' + uniq_id(), destination='postgres', dataset_name='drop_data_'+uniq_id())
    pipeline.run(source)

    table_names0 = list(pipeline.default_schema.tables)    
    helpers.drop(pipeline, resources=['droppable_a', 'droppable_b'])

    table_names1 = list(pipeline.default_schema.tables)
    pipeline.sync_destination()
    table_names2 = list(pipeline.default_schema.tables)    
    breakpoint()



    pass


if __name__ == '__main__':
    import pytest
    pytest.main(['-k', 'test_drop_command2', 'tests/load/pipeline/test_drop.py', '--pdb', '-s'])
