from typing import Any

import dlt
from dlt.common.utils import uniq_id
from dlt.pipeline import helpers, state_sync


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


if __name__ == '__main__':
    test_drop_command()
