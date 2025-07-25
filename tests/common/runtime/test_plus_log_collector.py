# run a pipeline with schema changes and check that the collector.on_schema_change is called

import os
from typing import Any, Dict, Generator, List
from unittest.mock import patch
from tests.utils import (
    preserve_environ,
    autouse_test_storage,
    unload_modules,
    wipe_pipeline,
    patch_home_dir,
)

import dlt
from dlt.common.runtime.plus_log_collector import PlusLogCollector


def test_on_schema_change() -> None:
    @dlt.resource(
        write_disposition="append",
        primary_key=["id"],
    )
    def identity_resource(
        data: List[Dict[str, Any]],
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """Identity resource that yields the data as-is"""
        yield data

    collector = PlusLogCollector()
    with patch.object(collector, "on_schema_change") as mock_on_schema_change:
        os.environ["COMPLETED_PROB"] = "1.0"
        pipeline = dlt.pipeline(destination="dummy", progress=collector)

        print("sotrage", pipeline._pipeline_storage.storage_path)

        data = [{"id": 1}, {"id": 2}]
        pipeline.run(identity_resource(data))
        # first run doesnt register as schema change
        assert mock_on_schema_change.call_count == 1

        # runwith identical data doesnt either
        data = [{"id": 1}, {"id": 2}]
        pipeline.run(identity_resource(data))
        assert mock_on_schema_change.call_count == 1

        # run with changed data will
        # once during load and once during run:
        data_with_schema_change = [{"id": 1, "nickname": "John"}, {"id": 2, "nickname": "Jane"}]
        pipeline.run(identity_resource(data_with_schema_change))
        assert mock_on_schema_change.call_count == 2
