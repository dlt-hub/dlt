from typing import Any, Iterator, Dict, Any, List
import secrets

import dlt
from dlt.extract.source import DltResource
from dlt.common.utils import uniq_id
from dlt.pipeline import helpers, state_sync
from dlt.pipeline.exceptions import PipelineHasPendingDataException
from dlt.common.pipeline import _resource_state
from dlt.destinations.job_client_impl import SqlJobClientBase
from tests.load.pipeline.utils import drop_pipeline


@dlt.source(section='droppable', name='droppable')
def droppable_source() -> List[DltResource]:
    @dlt.resource
    def droppable_a(a: dlt.sources.incremental[int]=dlt.sources.incremental('a', 0)) -> Iterator[Dict[str, Any]]:
        yield dict(a=1, b=2, c=3)
        yield dict(a=4, b=23, c=24)


    @dlt.resource
    def droppable_b(asd: dlt.sources.incremental[int]=dlt.sources.incremental('asd', 0)) -> Iterator[Dict[str, Any]]:
        # Child table
        yield dict(asd=2323, qe=555, items=[dict(m=1, n=2), dict(m=3, n=4)])


    @dlt.resource
    def droppable_c() -> Iterator[Dict[str, Any]]:
        # Grandchild table
        yield dict(asdasd=2424, qe=111, items=[
            dict(k=2, r=2, labels=[dict(name='abc'), dict(name='www')])
        ])

    @dlt.resource
    def droppable_d() -> Iterator[List[Dict[str, Any]]]:
        dlt.state()['data_from_d'] = {'foo1': {'bar': 1}, 'foo2': {'bar': 2}}
        yield [dict(o=55), dict(o=22)]

    return [droppable_a(), droppable_b(), droppable_c(), droppable_d()]


def test_drop_command_resources_and_state() -> None:
    """Test the drop command with resource and state path options and
    verify correct data is deleted from destination and locally"""
    source = droppable_source()
    pipeline = dlt.pipeline(pipeline_name='drop_test_' + uniq_id(), destination='postgres', dataset_name='drop_data_'+uniq_id())
    pipeline.run(source)

    attached = dlt.attach(pipeline.pipeline_name, pipeline.pipelines_dir)
    helpers.drop(attached, resources=['droppable_c', 'droppable_d'], state_paths='data_from_d.*.bar')

    attached2 = dlt.attach(pipeline.pipeline_name, pipeline.pipelines_dir)

    # Verify only requested resource tables are removed from pipeline schema
    expected_tables = {'droppable_a', 'droppable_b', 'droppable_b__items'}
    result_tables = set(t['name'] for t in attached2.default_schema.data_tables())
    assert result_tables == expected_tables

    # Verify requested tables are dropped from destination
    dropped_tables = {'droppable_c', 'droppable_c__items', 'droppable_c__items__labels', 'droppable_d'}
    client: SqlJobClientBase
    with attached2._get_destination_client(attached2.default_schema) as client:  # type: ignore[assignment]
        # Check all tables supposed to be dropped are not in dataset
        for table in dropped_tables:
            exists, _ = client.get_storage_table(table)
            assert not exists
        # Check tables not from dropped resources still exist
        for table in expected_tables:
            exists, _ = client.get_storage_table(table)
            assert exists

    # Verify only requested resource keys are removed from state
    sources_state = attached2.state['sources']  # type: ignore[typeddict-item]
    result_keys = set(sources_state['droppable']['resources'].keys())
    assert result_keys == {'droppable_a', 'droppable_b'}

    # Verify extra json paths are removed from state
    assert sources_state['droppable']['data_from_d'] == {'foo1': {}, 'foo2': {}}

    # Verify destination state matches the pipeline state
    with attached2.sql_client() as sql_client:
        destination_state = state_sync.load_state_from_destination(pipeline.pipeline_name, sql_client)
    pipeline_state = dict(attached2.state)
    del pipeline_state['_local']
    assert pipeline_state == destination_state


if __name__ == '__main__':
    import pytest
    pytest.main(['-k', 'drop_command_general', 'tests/load/pipeline/test_drop.py', '--pdb', '-s'])
