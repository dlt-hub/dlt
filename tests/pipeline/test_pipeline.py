import asyncio
import pathlib
from concurrent.futures import ThreadPoolExecutor
import itertools
import logging
import os
import random
import shutil
import threading
from time import sleep
from typing import Any, List, Tuple, cast
from tenacity import retry_if_exception, Retrying, stop_after_attempt

import pytest
from dlt.common.storages import FileStorage

import dlt
from dlt.common import json, pendulum
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import ConfigFieldMissingException, InvalidNativeValue
from dlt.common.data_writers.exceptions import FileImportNotFound, SpecLookupFailed
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import WithStateSync
from dlt.common.destination.exceptions import (
    DestinationHasFailedJobs,
    DestinationIncompatibleLoaderFileFormatException,
    DestinationLoadingViaStagingNotSupported,
    DestinationNoStagingMode,
    DestinationTerminalException,
    UnknownDestinationModule,
)
from dlt.common.exceptions import PipelineStateNotAvailable
from dlt.common.pipeline import LoadInfo, PipelineContext
from dlt.common.runtime.collector import LogCollector
from dlt.common.schema.exceptions import TableIdentifiersFrozen
from dlt.common.schema.typing import TColumnSchema
from dlt.common.schema.utils import new_column, new_table
from dlt.common.typing import DictStrAny
from dlt.common.utils import uniq_id
from dlt.common.schema import Schema

from dlt.destinations import filesystem, redshift, dummy, duckdb
from dlt.destinations.impl.filesystem.filesystem import INIT_FILE_NAME
from dlt.extract.exceptions import InvalidResourceDataTypeBasic, PipeGenInvalid, SourceExhausted
from dlt.extract.extract import ExtractStorage
from dlt.extract import DltResource, DltSource
from dlt.extract.extractors import MaterializedEmptyList
from dlt.load.exceptions import LoadClientJobFailed
from dlt.normalize.exceptions import NormalizeJobFailed
from dlt.pipeline.exceptions import InvalidPipelineName, PipelineNotActive, PipelineStepFailed
from dlt.pipeline.helpers import retry_load

from dlt.pipeline.pipeline import Pipeline
from tests.common.utils import TEST_SENTRY_DSN
from tests.utils import TEST_STORAGE_ROOT
from tests.extract.utils import expect_extracted_file
from tests.pipeline.utils import (
    assert_data_table_counts,
    assert_load_info,
    airtable_emojis,
    assert_only_table_columns,
    load_data_table_counts,
    load_tables_to_dicts,
    many_delayed,
)

DUMMY_COMPLETE = dummy(completed_prob=1)  # factory set up to complete jobs


def test_default_pipeline() -> None:
    p = dlt.pipeline()
    # this is a name of executing test harness or blank pipeline on windows
    possible_names = ["dlt_pytest", "dlt_pipeline"]
    possible_dataset_names = ["dlt_pytest_dataset", "dlt_pipeline_dataset"]
    assert p.pipeline_name in possible_names
    assert p.pipelines_dir == os.path.abspath(os.path.join(TEST_STORAGE_ROOT, ".dlt", "pipelines"))
    assert p.runtime_config.pipeline_name == p.pipeline_name
    # dataset that will be used to load data is the pipeline name
    assert p.dataset_name in possible_dataset_names
    assert p.destination is None
    assert p.default_schema_name is None

    # this is the same pipeline
    p2 = dlt.pipeline()
    assert p is p2

    # this will create default schema
    p.extract(["a", "b", "c"], table_name="data")
    # `_pipeline` is removed from default schema name
    assert p.default_schema_name in ["dlt_pytest", "dlt"]


def test_default_pipeline_dataset_layout(environment) -> None:
    # Set dataset_name_layout to "bobby_%s"
    dataset_name_layout = "bobby_%s"
    environment["DATASET_NAME_LAYOUT"] = dataset_name_layout

    p = dlt.pipeline()
    # this is a name of executing test harness or blank pipeline on windows
    possible_names = ["dlt_pytest", "dlt_pipeline"]
    possible_dataset_names = [
        dataset_name_layout % "dlt_pytest_dataset",
        dataset_name_layout % "dlt_pipeline_dataset",
    ]
    assert p.pipeline_name in possible_names
    assert p.pipelines_dir == os.path.abspath(os.path.join(TEST_STORAGE_ROOT, ".dlt", "pipelines"))
    assert p.runtime_config.pipeline_name == p.pipeline_name
    # dataset that will be used to load data is the pipeline name
    assert p.dataset_name in possible_dataset_names
    assert p.destination is None
    assert p.default_schema_name is None

    # this is the same pipeline
    p2 = dlt.pipeline()
    assert p is p2

    # this will create default schema
    p.extract(["a", "b", "c"], table_name="data")
    # `_pipeline` is removed from default schema name
    assert p.default_schema_name in ["dlt_pytest", "dlt"]


def test_default_pipeline_dataset() -> None:
    # dummy does not need a dataset
    p = dlt.pipeline(destination="dummy")
    assert p.dataset_name is None  # so it is none

    # filesystem needs one
    possible_dataset_names = ["dlt_pytest_dataset", "dlt_pipeline_dataset"]
    p = dlt.pipeline(destination="filesystem")
    assert p.dataset_name in possible_dataset_names


def test_default_pipeline_dataset_name(environment) -> None:
    environment["DATASET_NAME"] = "dataset"
    environment["DATASET_NAME_LAYOUT"] = "prefix_%s"

    p = dlt.pipeline(destination="filesystem")
    assert p.dataset_name == "prefix_dataset"


def test_default_pipeline_dataset_layout_exception(environment) -> None:
    # Set dataset_name_layout without placeholder %s
    environment["DATASET_NAME_LAYOUT"] = "bobby_"

    with pytest.raises(ValueError):
        dlt.pipeline(destination="filesystem")


def test_default_pipeline_dataset_layout_placeholder(environment) -> None:
    # Set dataset_name_layout only with placeholder
    environment["DATASET_NAME_LAYOUT"] = "%s"

    possible_dataset_names = ["dlt_pytest_dataset", "dlt_pipeline_dataset"]
    p = dlt.pipeline(destination="filesystem")
    assert p.dataset_name in possible_dataset_names


def test_default_pipeline_dataset_layout_empty(environment) -> None:
    # Set dataset_name_layout empty
    environment["DATASET_NAME_LAYOUT"] = ""

    possible_dataset_names = ["dlt_pytest_dataset", "dlt_pipeline_dataset"]
    p = dlt.pipeline(destination="filesystem")
    assert p.dataset_name in possible_dataset_names


def test_run_dev_mode_default_dataset() -> None:
    p = dlt.pipeline(dev_mode=True, destination="filesystem")
    assert p.dataset_name.endswith(p._pipeline_instance_id)
    # restore this pipeline
    r_p = dlt.attach(dev_mode=False)
    assert r_p.dataset_name.endswith(p._pipeline_instance_id)

    # dummy does not need dataset
    p = dlt.pipeline(dev_mode=True, destination="dummy")
    assert p.dataset_name is None
    # simulate set new dataset
    p._set_destinations("filesystem")
    assert p.dataset_name is None
    p._set_dataset_name(None)
    # full refresh is still observed
    assert p.dataset_name and p.dataset_name.endswith(p._pipeline_instance_id)


def test_run_dev_mode_default_dataset_layout(environment) -> None:
    # Set dataset_name_layout to "bobby_%s"
    dataset_name_layout = "bobby_%s"
    environment["DATASET_NAME_LAYOUT"] = dataset_name_layout

    p = dlt.pipeline(dev_mode=True, destination="filesystem")
    assert p.dataset_name in [
        dataset_name_layout % f"dlt_pytest_dataset{p._pipeline_instance_id}",
        dataset_name_layout % f"dlt_pipeline_dataset{p._pipeline_instance_id}",
    ]
    # restore this pipeline
    r_p = dlt.attach(dev_mode=False)
    assert r_p.dataset_name in [
        dataset_name_layout % f"dlt_pytest_dataset{p._pipeline_instance_id}",
        dataset_name_layout % f"dlt_pipeline_dataset{p._pipeline_instance_id}",
    ]

    # dummy does not need dataset
    p = dlt.pipeline(dev_mode=True, destination="dummy")
    assert p.dataset_name is None

    # simulate set new dataset
    p._set_destinations("filesystem")
    assert p.dataset_name is None
    p._set_dataset_name(None)

    # full refresh is still observed
    assert p.dataset_name in [
        dataset_name_layout % f"dlt_pytest_dataset{p._pipeline_instance_id}",
        dataset_name_layout % f"dlt_pipeline_dataset{p._pipeline_instance_id}",
    ]


def test_run_dev_mode_underscored_dataset() -> None:
    p = dlt.pipeline(dev_mode=True, dataset_name="_main_")
    assert p.dataset_name.endswith(p._pipeline_instance_id)
    # restore this pipeline
    r_p = dlt.attach(dev_mode=False)
    assert r_p.dataset_name.endswith(p._pipeline_instance_id)


def test_pipeline_with_non_alpha_name() -> None:
    name = "another pipeline %__8329イロハニホヘト"
    # contains %
    with pytest.raises(InvalidPipelineName):
        p = dlt.pipeline(pipeline_name=name)

    name = "another pipeline __8329イロハニホヘト"
    p = dlt.pipeline(pipeline_name=name)
    assert p.pipeline_name == name
    # default dataset is set
    assert p.dataset_name == f"{name}_dataset"
    # also pipeline name in runtime must be correct
    assert p.runtime_config.pipeline_name == p.pipeline_name

    # this will create default schema
    p.extract(["a", "b", "c"], table_name="data")
    assert p.default_schema_name == "another_pipeline_8329x"


def test_file_format_resolution() -> None:
    # raise on destinations that does not support staging
    with pytest.raises(DestinationLoadingViaStagingNotSupported):
        dlt.pipeline(
            pipeline_name="managed_state_pipeline", destination="postgres", staging="filesystem"
        )

    # raise on staging that does not support staging interface
    with pytest.raises(DestinationNoStagingMode):
        dlt.pipeline(pipeline_name="managed_state_pipeline", staging="postgres")

    # check invalid input
    with pytest.raises(DestinationIncompatibleLoaderFileFormatException):
        pipeline = dlt.pipeline(pipeline_name="managed_state_pipeline", destination="postgres")
        pipeline.config.restore_from_destination = False
        pipeline.run([1, 2, 3], table_name="numbers", loader_file_format="parquet")

    # check invalid input
    with pytest.raises(DestinationIncompatibleLoaderFileFormatException):
        pipeline = dlt.pipeline(
            pipeline_name="managed_state_pipeline", destination="athena", staging="filesystem"
        )
        pipeline.config.restore_from_destination = False
        pipeline.run([1, 2, 3], table_name="numbers", loader_file_format="insert_values")


def test_invalid_dataset_name() -> None:
    # this is invalid dataset name but it will be normalized within a destination
    p = dlt.pipeline(dataset_name="!")
    assert p.dataset_name == "!"


def test_invalid_dataset_layout(environment) -> None:
    # Set dataset_name_prefix to "bobby"
    dataset_name_layout = "bobby_%s"
    environment["DATASET_NAME_LAYOUT"] = dataset_name_layout

    # this is invalid dataset name but it will be normalized within a destination
    p = dlt.pipeline(dataset_name="!")
    assert p.dataset_name == dataset_name_layout % "!"


def test_pipeline_context_deferred_activation() -> None:
    ctx = Container()[PipelineContext]
    assert ctx.is_active() is False
    # this creates default pipeline
    p = ctx.pipeline()
    # and we can get it here
    assert p is dlt.pipeline()


def test_pipeline_context() -> None:
    ctx = Container()[PipelineContext]
    assert ctx.is_active() is False
    # create pipeline
    p = dlt.pipeline()
    assert ctx.is_active() is True
    assert ctx.pipeline() is p
    assert p.is_active is True
    # has no destination context
    assert DestinationCapabilitiesContext not in Container()

    # create another pipeline
    p2 = dlt.pipeline(pipeline_name="another pipeline", destination="duckdb")
    assert ctx.pipeline() is p2
    assert p.is_active is False
    assert p2.is_active is True

    p3 = dlt.pipeline(pipeline_name="more pipelines", destination="dummy")
    assert ctx.pipeline() is p3
    assert p3.is_active is True
    assert p2.is_active is False
    # no default naming convention
    assert Container()[DestinationCapabilitiesContext].naming_convention is None

    # restore previous
    p2 = dlt.attach("another pipeline")
    assert ctx.pipeline() is p2
    assert p3.is_active is False
    assert p2.is_active is True


def test_import_unknown_destination() -> None:
    with pytest.raises(UnknownDestinationModule):
        dlt.pipeline(destination="dlt.destinations.unknown")


def test_configured_destination_type(environment) -> None:
    environment["DESTINATION_TYPE"] = "dlt.destinations.postgres"
    environment["PIPELINE_NAME"] = "postgres_pipe"

    p = dlt.pipeline()
    assert p.destination is not None
    assert p.destination.destination_type == "dlt.destinations.postgres"
    assert p.destination.destination_name == "postgres"
    assert p.pipeline_name == "postgres_pipe"


def test_configured_destination_unknown_type(environment) -> None:
    environment["DESTINATION_TYPE"] = "dlt.destinations.unknown"

    with pytest.raises(UnknownDestinationModule):
        dlt.pipeline()


def test_configured_destination_unknown_name(environment) -> None:
    environment["DESTINATION_NAME"] = "filesystem-prod"
    environment["DESTINATION_TYPE"] = "filesystem"

    p = dlt.pipeline()
    assert p.destination is not None
    assert p.destination.destination_type == "dlt.destinations.filesystem"
    assert p.destination.destination_name == "filesystem-prod"

    # we do not have config for postgres-prod so getting destination client must fail
    with pytest.raises(ConfigFieldMissingException):
        p.destination_client()


def test_deterministic_salt(environment) -> None:
    environment["PIPELINE_NAME"] = "postgres_pipe"
    p = dlt.pipeline()
    p2 = dlt.attach()
    assert p.pipeline_name == p2.pipeline_name == "postgres_pipe"
    assert p.pipeline_salt == p2.pipeline_salt

    p3 = dlt.pipeline(pipeline_name="postgres_redshift")
    assert p.pipeline_salt != p3.pipeline_salt


def test_destination_explicit_credentials(environment: Any) -> None:
    from dlt.destinations import motherduck

    # test redshift
    p = dlt.pipeline(
        pipeline_name="postgres_pipeline",
        destination=motherduck(credentials="md://user:password@/dlt_data"),
    )
    config = p.destination_client().config
    assert config.credentials.is_resolved()
    assert config.credentials.to_native_representation() == "md://user:password@/dlt_data"


def test_destination_staging_config(environment: Any) -> None:
    fs_dest = filesystem("file:///testing-bucket")
    p = dlt.pipeline(
        pipeline_name="staging_pipeline",
        destination=redshift(credentials="redshift://loader:loader@localhost:5432/dlt_data"),
        staging=fs_dest,
    )
    schema = Schema("foo")
    p._inject_schema(schema)
    initial_config = p._get_destination_client_initial_config(p.staging, as_staging=True)
    staging_config = fs_dest.configuration(initial_config)  # type: ignore[arg-type]

    # Ensure that as_staging flag is set in the final resolved conifg
    assert staging_config.as_staging_destination is True


def test_destination_factory_defaults_resolve_from_config(environment: Any) -> None:
    """Params passed explicitly to destination supersede config values.
    Env config values supersede default values.
    """
    environment["FAIL_PROB"] = "0.3"
    environment["RETRY_PROB"] = "0.8"
    p = dlt.pipeline(pipeline_name="dummy_pipeline", destination=dummy(retry_prob=0.5))

    client = p.destination_client()

    assert client.config.fail_prob == 0.3  # type: ignore[attr-defined]
    assert client.config.retry_prob == 0.5  # type: ignore[attr-defined]


def test_destination_credentials_in_factory(environment: Any) -> None:
    os.environ["DESTINATION__REDSHIFT__CREDENTIALS"] = "redshift://abc:123@localhost:5432/some_db"

    redshift_dest = redshift("redshift://abc:123@localhost:5432/other_db")

    p = dlt.pipeline(pipeline_name="dummy_pipeline", destination=redshift_dest)

    initial_config = p._get_destination_client_initial_config(p.destination)
    dest_config = redshift_dest.configuration(initial_config)  # type: ignore[arg-type]
    # Explicit factory arg supersedes config
    assert dest_config.credentials.database == "other_db"

    redshift_dest = redshift()
    p = dlt.pipeline(pipeline_name="dummy_pipeline", destination=redshift_dest)

    initial_config = p._get_destination_client_initial_config(p.destination)
    dest_config = redshift_dest.configuration(initial_config)  # type: ignore[arg-type]
    assert dest_config.credentials.database == "some_db"


def test_destination_explicit_invalid_credentials_filesystem(environment: Any) -> None:
    # if string cannot be parsed
    p = dlt.pipeline(
        pipeline_name="postgres_pipeline",
        destination=filesystem(bucket_url="s3://test", destination_name="uniq_s3_bucket"),
    )
    with pytest.raises(PipelineStepFailed) as pip_ex:
        p.run([1, 2, 3], table_name="data", credentials="PR8BLEM")
    assert isinstance(pip_ex.value.__cause__, InvalidNativeValue)


def test_extract_source_twice() -> None:
    def some_data():
        yield [1, 2, 3]
        yield [1, 2, 3]

    s = DltSource(dlt.Schema("source"), "module", [dlt.resource(some_data())])
    dlt.pipeline().extract(s)
    with pytest.raises(PipelineStepFailed) as py_ex:
        dlt.pipeline().extract(s)
    assert type(py_ex.value.exception) == SourceExhausted
    assert py_ex.value.exception.source_name == "source"


def test_disable_enable_state_sync(environment: Any) -> None:
    environment["RESTORE_FROM_DESTINATION"] = "False"
    p = dlt.pipeline(destination="redshift")

    def some_data():
        yield [1, 2, 3]

    s = DltSource(dlt.Schema("default"), "module", [dlt.resource(some_data())])
    dlt.pipeline().extract(s)
    storage = ExtractStorage(p._normalize_storage_config())
    assert len(storage.list_files_to_normalize_sorted()) == 1
    expect_extracted_file(storage, "default", "some_data", json.dumps([1, 2, 3]))
    with pytest.raises(FileNotFoundError):
        expect_extracted_file(storage, "default", s.schema.state_table_name, "")

    p.config.restore_from_destination = True
    # extract to different schema, state must go to default schema
    s = DltSource(dlt.Schema("default_2"), "module", [dlt.resource(some_data())])
    dlt.pipeline().extract(s)
    expect_extracted_file(storage, "default", s.schema.state_table_name, "***")


def test_extract_multiple_sources() -> None:
    s1 = DltSource(
        dlt.Schema("default"),
        "module",
        [dlt.resource([1, 2, 3], name="resource_1"), dlt.resource([3, 4, 5], name="resource_2")],
    )
    s2 = DltSource(
        dlt.Schema("default_2"),
        "module",
        [dlt.resource([6, 7, 8], name="resource_3"), dlt.resource([9, 10, 0], name="resource_4")],
    )

    p = dlt.pipeline(destination="dummy")
    p.config.restore_from_destination = False
    p.extract([s1, s2])
    storage = ExtractStorage(p._normalize_storage_config())
    expect_extracted_file(storage, "default", "resource_1", json.dumps([1, 2, 3]))
    expect_extracted_file(storage, "default", "resource_2", json.dumps([3, 4, 5]))
    expect_extracted_file(storage, "default_2", "resource_3", json.dumps([6, 7, 8]))
    expect_extracted_file(storage, "default_2", "resource_4", json.dumps([9, 10, 0]))
    assert len(storage.list_files_to_normalize_sorted()) == 4
    p.normalize()

    # make the last resource fail

    @dlt.resource
    def i_fail():
        raise NotImplementedError()

    s3 = DltSource(
        dlt.Schema("default_3"),
        "module",
        [dlt.resource([1, 2, 3], name="resource_1"), dlt.resource([3, 4, 5], name="resource_2")],
    )
    s4 = DltSource(
        dlt.Schema("default_4"), "module", [dlt.resource([6, 7, 8], name="resource_3"), i_fail]
    )

    with pytest.raises(PipelineStepFailed):
        # NOTE: if you swap s3 and s4 the test on list_schemas will fail: s3 will extract normally and update live schemas, s4 will break exec later
        p.extract([s4, s3])

    # nothing to normalize
    assert len(storage.list_files_to_normalize_sorted()) == 0
    # pipeline state is successfully rollbacked after the last extract and default_3 and 4 schemas are not present
    assert set(p.schema_names) == {"default", "default_2"}
    assert set(p._schema_storage.list_schemas()) == {"default", "default_2"}


def test_mark_hints() -> None:
    # this resource emits table schema with first item
    @dlt.resource
    def with_mark():
        yield dlt.mark.with_hints(
            {"id": 1},
            dlt.mark.make_hints(
                table_name="spec_table", write_disposition="merge", primary_key="id"
            ),
        )
        yield {"id": 2}

    p = dlt.pipeline(destination="dummy", pipeline_name="mark_pipeline")
    p.extract(with_mark())
    storage = ExtractStorage(p._normalize_storage_config())
    expect_extracted_file(storage, "mark", "spec_table", json.dumps([{"id": 1}, {"id": 2}]))
    p.normalize()
    # no "with_mark" table in the schema: we update resource hints before any table schema is computed
    assert "with_mark" not in p.default_schema.tables
    assert "spec_table" in p.default_schema.tables
    # resource name is kept
    assert p.default_schema.tables["spec_table"]["resource"] == "with_mark"


def test_mark_hints_with_variant() -> None:
    @dlt.resource(primary_key="pk")
    def with_table_hints():
        # dispatch to table a
        yield dlt.mark.with_hints(
            {"id": 1, "pk": "A"},
            dlt.mark.make_hints(
                table_name="table_a", columns=[{"name": "id", "data_type": "bigint"}]
            ),
            create_table_variant=True,
        )

        # dispatch to table b
        yield dlt.mark.with_hints(
            {"id": 2, "pk": "B"},
            dlt.mark.make_hints(table_name="table_b", write_disposition="replace"),
            create_table_variant=True,
        )

        # item to resource
        yield {"id": 3, "pk": "C"}
        # table a with table_hints
        yield dlt.mark.with_table_name({"id": 4, "pk": "D"}, "table_a")
        # table b with table_hints
        yield dlt.mark.with_table_name({"id": 5, "pk": "E"}, "table_b")

    pipeline_name = "pipe_" + uniq_id()
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="duckdb")
    info = pipeline.run(with_table_hints)
    assert_load_info(info)
    assert pipeline.last_trace.last_normalize_info.row_counts == {
        "_dlt_pipeline_state": 1,
        "table_a": 2,
        "table_b": 2,
        "with_table_hints": 1,
    }
    # check table counts
    assert_data_table_counts(pipeline, {"table_a": 2, "table_b": 2, "with_table_hints": 1})


def test_mark_hints_variant_dynamic_name() -> None:
    @dlt.resource(table_name=lambda item: "table_" + item["tag"])
    def with_table_hints():
        # dispatch to table a
        yield dlt.mark.with_hints(
            {"id": 1, "pk": "A", "tag": "a"},
            dlt.mark.make_hints(
                table_name="table_a",
                primary_key="pk",
                columns=[{"name": "id", "data_type": "bigint"}],
            ),
            create_table_variant=True,
        )

        # dispatch to table b
        yield dlt.mark.with_hints(
            {"id": 2, "pk": "B", "tag": "b"},
            dlt.mark.make_hints(table_name="table_b", write_disposition="replace"),
            create_table_variant=True,
        )

        # dispatch by tag
        yield {"id": 3, "pk": "C", "tag": "c"}
        yield {"id": 4, "pk": "D", "tag": "a"}
        yield {"id": 5, "pk": "E", "tag": "b"}

    pipeline_name = "pipe_" + uniq_id()
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="duckdb")
    info = pipeline.run(with_table_hints)
    assert_load_info(info)
    assert pipeline.last_trace.last_normalize_info.row_counts == {
        "_dlt_pipeline_state": 1,
        "table_a": 2,
        "table_b": 2,
        "table_c": 1,
    }
    # check table counts
    assert_data_table_counts(pipeline, {"table_a": 2, "table_b": 2, "table_c": 1})


def test_restore_state_on_dummy() -> None:
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination=DUMMY_COMPLETE)
    p.config.restore_from_destination = True
    info = p.run([1, 2, 3], table_name="dummy_table")
    print(info)
    assert p.first_run is False
    # no effect
    p.sync_destination()
    assert p.state["_state_version"] == 1

    # wipe out storage
    p._wipe_working_folder()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination=DUMMY_COMPLETE)
    assert p.first_run is True
    p.sync_destination()
    assert p.first_run is True
    assert p.state["_state_version"] == 0


def test_first_run_flag() -> None:
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination=DUMMY_COMPLETE)
    assert p.first_run is True
    # attach
    p = dlt.attach(pipeline_name=pipeline_name)
    assert p.first_run is True
    p.extract([1, 2, 3], table_name="dummy_table")
    assert p.first_run is True
    # attach again
    p = dlt.attach(pipeline_name=pipeline_name, destination=DUMMY_COMPLETE)
    assert p.first_run is True
    assert len(p.list_extracted_load_packages()) > 0
    p.normalize()
    assert len(p.list_normalized_load_packages()) > 0
    assert p.first_run is True
    # load will change the flag
    p.load()
    assert p.first_run is False
    # attach again
    p = dlt.attach(pipeline_name=pipeline_name)
    assert p.first_run is False
    # wipe the pipeline
    p._create_pipeline()
    assert p.first_run is True
    p._save_state(p._get_state())
    p = dlt.attach(pipeline_name=pipeline_name)
    assert p.first_run is True


def test_has_pending_data_flag() -> None:
    p = dlt.pipeline(pipeline_name="pipe_" + uniq_id(), destination=DUMMY_COMPLETE)
    assert p.has_pending_data is False
    p.extract([1, 2, 3], table_name="dummy_table")
    assert p.has_pending_data is True
    p.normalize()
    assert p.has_pending_data is True
    p.load()
    assert p.has_pending_data is False


def test_sentry_tracing() -> None:
    import sentry_sdk

    os.environ["RUNTIME__SENTRY_DSN"] = TEST_SENTRY_DSN

    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination=DUMMY_COMPLETE)

    # def inspect_transaction(ctx):
    #     print(ctx)
    #     return 1.0

    # sentry_sdk.Hub.current.client.options["traces_sampler"] = inspect_transaction

    # def inspect_events(event, hint):
    #     print(event)
    #     print(hint)
    #     return event

    # sentry_sdk.Hub.current.client.options["before_send"] = inspect_events

    @dlt.resource
    def r_check_sentry():
        assert sentry_sdk.Scope.get_current_scope().span.op == "extract"
        assert sentry_sdk.Scope.get_current_scope().transaction.name == "run"
        yield [1, 2, 3]

    p.run(r_check_sentry)
    assert sentry_sdk.Hub.current.scope.span is None
    sentry_sdk.flush()

    @dlt.resource
    def r_fail():
        raise NotImplementedError()

    # run pipeline with error in extract
    with pytest.raises(PipelineStepFailed) as py_ex:
        p.run(r_fail)
    assert py_ex.value.step == "extract"
    # sentry cleaned up
    assert sentry_sdk.Hub.current.scope.span is None

    # run pipeline with error in load
    os.environ["FAIL_SCHEMA_UPDATE"] = "true"
    with pytest.raises(PipelineStepFailed) as py_ex:
        p.run(r_check_sentry)
    assert py_ex.value.step == "load"
    assert sentry_sdk.Hub.current.scope.span is None


def test_pipeline_state_on_extract_exception() -> None:
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")

    @dlt.resource
    def data_piece_1():
        yield [1, 2, 3]
        yield [3, 4, 5]

    @dlt.resource
    def data_piece_2():
        yield [6, 7, 8]
        raise NotImplementedError()

    with pytest.raises(PipelineStepFailed) as pip_ex:
        p.run([data_piece_1, data_piece_2], write_disposition="replace")
    # male sure that exception has right step info
    assert pip_ex.value.load_id in pip_ex.value.step_info.loads_ids
    # print(pip_ex.value.load_id)
    # print(pip_ex.value.step_info.asdict())
    # print(p._last_trace.last_pipeline_step_trace("extract").exception_traces)

    # first run didn't really happen
    assert p.first_run is True
    assert p.has_data is False
    assert p.default_schema_name is None
    # live schemas created during extract are popped from mem
    assert len(p._schema_storage.list_schemas()) == 0

    # restore the pipeline
    p = dlt.attach(pipeline_name)
    assert p.first_run is True
    assert p.has_data is False
    # no schema was saved to storage, the one above was only in memory
    assert p._schema_storage.list_schemas() == []
    assert p.default_schema_name is None

    # same but with multiple sources generating many schemas

    @dlt.source
    def data_schema_1():
        return data_piece_1

    @dlt.source
    def data_schema_2():
        return data_piece_1

    @dlt.source
    def data_schema_3():
        return data_piece_2

    # new pipeline
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination=DUMMY_COMPLETE)

    with pytest.raises(PipelineStepFailed):
        p.run([data_schema_1(), data_schema_2(), data_schema_3()], write_disposition="replace")

    # first run didn't really happen
    assert p.first_run is True
    assert p.has_data is False
    # live schemas created during extract are popped from mem
    assert len(p._schema_storage.list_schemas()) == 0
    assert p.default_schema_name is None

    p.run([data_schema_1(), data_schema_2()], write_disposition="replace")
    assert set(p.schema_names) == set(p._schema_storage.list_schemas())


def test_run_with_table_name_exceeding_path_length() -> None:
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name)

    # we must fix that
    with pytest.raises(PipelineStepFailed) as sf_ex:
        p.extract([1, 2, 3], table_name="TABLE_" + "a" * 267)
    assert isinstance(sf_ex.value.__context__, OSError)


def test_raise_on_failed_job() -> None:
    os.environ["FAIL_PROB"] = "1.0"
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")
    with pytest.raises(PipelineStepFailed) as py_ex:
        p.run([1, 2, 3], table_name="numbers")
    assert py_ex.value.step == "load"
    # get package info
    package_info = p.get_load_package_info(py_ex.value.step_info.loads_ids[0])
    assert package_info.state == "aborted"
    assert isinstance(py_ex.value.__context__, LoadClientJobFailed)
    assert isinstance(py_ex.value.__context__, DestinationTerminalException)
    # next call to run does nothing
    load_info = p.run()
    assert load_info is None


def test_load_info_raise_on_failed_jobs() -> None:
    # By default, raises terminal error on a failed job and aborts load. This pipeline does not fail
    os.environ["COMPLETED_PROB"] = "1.0"
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")
    load_info = p.run([1, 2, 3], table_name="numbers")
    assert load_info.has_failed_jobs is False

    # Test explicit raising on a failed job after the load is completed. Let pipeline fail
    os.environ["COMPLETED_PROB"] = "0.0"
    os.environ["FAIL_PROB"] = "1.0"
    os.environ["RAISE_ON_FAILED_JOBS"] = "false"
    load_info = p.run([1, 2, 3], table_name="numbers")
    assert load_info.has_failed_jobs is True
    with pytest.raises(DestinationHasFailedJobs) as py_ex:
        load_info.raise_on_failed_jobs()
    assert py_ex.value.destination_name == "dummy"
    assert py_ex.value.load_id == load_info.loads_ids[0]

    # Test automatic raising on a failed job which aborts the load. Let pipeline fail
    os.environ["RAISE_ON_FAILED_JOBS"] = "true"
    with pytest.raises(PipelineStepFailed) as py_ex_2:
        p.run([1, 2, 3], table_name="numbers")
    load_info = py_ex_2.value.step_info  # type: ignore[assignment]
    assert load_info.has_failed_jobs is True
    with pytest.raises(DestinationHasFailedJobs) as py_ex:
        load_info.raise_on_failed_jobs()
    assert py_ex.value.destination_name == "dummy"
    assert py_ex.value.load_id == load_info.loads_ids[0]


def test_run_load_pending() -> None:
    # prepare some data and complete load with run
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination=DUMMY_COMPLETE)

    def some_data():
        yield from [1, 2, 3]

    @dlt.source
    def source():
        return dlt.resource(some_data(), name="numbers")

    s = source()
    p.extract(s)
    assert s.exhausted
    # will normalize and load, the data, the source will not be evaluated so there's no exception
    load_info = p.run(s)
    assert len(load_info.loads_ids) == 1
    # now it is
    with pytest.raises(PipelineStepFailed) as py_ex:
        p.run(s)
    assert isinstance(py_ex.value.__context__, SourceExhausted)

    # now only load
    s = source()
    p.extract(s)
    p.normalize()
    load_info = p.run(s)
    assert len(load_info.loads_ids) == 1


def test_retry_load() -> None:
    os.environ["COMPLETED_PROB"] = "1.0"
    retry_count = 2

    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")

    @dlt.resource
    def fail_extract():
        nonlocal retry_count
        retry_count -= 1
        if retry_count == 0:
            yield [1, 2, 3]
        else:
            raise Exception("Transient")

    attempt = None

    for attempt in Retrying(
        stop=stop_after_attempt(3),
        retry=retry_if_exception(retry_load(("load", "extract"))),
        reraise=True,
    ):
        with attempt:
            p.run(fail_extract())
    # it retried
    assert retry_count == 0

    # now it fails (extract is terminal exception)
    retry_count = 2
    with pytest.raises(PipelineStepFailed) as py_ex:
        for attempt in Retrying(
            stop=stop_after_attempt(3), retry=retry_if_exception(retry_load(())), reraise=True
        ):
            with attempt:
                p.run(fail_extract())
    assert isinstance(py_ex.value, PipelineStepFailed)
    assert py_ex.value.step == "extract"

    os.environ["COMPLETED_PROB"] = "0.0"
    os.environ["FAIL_PROB"] = "1.0"
    with pytest.raises(PipelineStepFailed) as py_ex:
        for attempt in Retrying(
            stop=stop_after_attempt(3),
            retry=retry_if_exception(retry_load(("load", "extract"))),
            reraise=True,
        ):
            with attempt:
                p.run(fail_extract())
    assert isinstance(py_ex.value, PipelineStepFailed)
    assert py_ex.value.step == "load"


@pytest.mark.skip("Not implemented")
def test_extract_exception() -> None:
    # make sure that PipelineStepFailed contains right step information
    # TODO: same tests for normalize and load
    pass


@pytest.mark.skip("Not implemented")
def test_extract_all_data_types() -> None:
    # list, iterators, generators, resource, source, list of resources, list of sources
    pass


def test_set_get_local_value() -> None:
    p = dlt.pipeline(destination="dummy", dev_mode=True)
    value = uniq_id()
    # value is set
    p.set_local_state_val(value, value)
    assert p.get_local_state_val(value) == value
    # check if this is actual local state
    assert p.state["_local"][value] == value  # type: ignore[literal-required]

    new_val = uniq_id()

    # check in context manager
    @dlt.resource
    def _w_local_state():
        # join existing managed state
        p.set_local_state_val(new_val, new_val)
        yield 1

    p.extract(_w_local_state)
    assert p.state["_local"][new_val] == new_val  # type: ignore[literal-required]


def test_changed_write_disposition() -> None:
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination=DUMMY_COMPLETE)

    @dlt.resource
    def resource_1():
        yield [1, 2, 3]

    p.run(resource_1, write_disposition="append")
    assert p.default_schema.get_table("resource_1")["write_disposition"] == "append"

    p.run(resource_1, write_disposition="append")
    assert p.default_schema.get_table("resource_1")["write_disposition"] == "append"

    p.run(resource_1, write_disposition="replace")
    print(list(p._schema_storage.live_schemas.values())[0].to_pretty_yaml())
    assert (
        p.schemas[p.default_schema_name].get_table("resource_1")["write_disposition"] == "replace"
    )
    assert p.default_schema.get_table("resource_1")["write_disposition"] == "replace"


@dlt.transformer(
    name="github_repo_events",
    primary_key="id",
    write_disposition="merge",
    table_name=lambda i: i["type"],
)
def github_repo_events(page):
    yield page


@dlt.transformer(name="github_repo_events", primary_key="id", write_disposition="merge")
def github_repo_events_table_meta(page):
    yield from [dlt.mark.with_table_name(p, p["type"]) for p in page]


@dlt.resource
def _get_shuffled_events(repeat: int = 1):
    for _ in range(repeat):
        with open(
            "tests/normalize/cases/github.events.load_page_1_duck.json", "r", encoding="utf-8"
        ) as f:
            issues = json.load(f)
            yield issues


@pytest.mark.parametrize("github_resource", (github_repo_events_table_meta, github_repo_events))
def test_dispatch_rows_to_tables(github_resource: DltResource):
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination=DUMMY_COMPLETE)

    info = p.run(_get_shuffled_events | github_resource)
    assert_load_info(info)

    # get all expected tables
    events = list(_get_shuffled_events)
    expected_tables = set(
        map(lambda e: p.default_schema.naming.normalize_identifier(e["type"]), events)
    )

    # all the tables present
    assert (
        expected_tables.intersection([t["name"] for t in p.default_schema.data_tables()])
        == expected_tables
    )

    # all the columns have primary keys and merge disposition derived from resource
    for table in p.default_schema.data_tables():
        if table.get("parent") is None:
            assert table["write_disposition"] == "merge"
            assert table["columns"]["id"]["primary_key"] is True


def test_resource_name_in_schema() -> None:
    @dlt.resource(table_name="some_table")
    def static_data():
        yield {"a": 1, "b": 2}

    @dlt.resource(table_name=lambda x: "dynamic_func_table")
    def dynamic_func_data():
        yield {"a": 1, "b": 2}

    @dlt.resource
    def dynamic_mark_data():
        yield dlt.mark.with_table_name({"a": 1, "b": 2}, "dynamic_mark_table")

    @dlt.resource(table_name="parent_table")
    def nested_data():
        yield {"a": 1, "items": [{"c": 2}, {"c": 3}, {"c": 4}]}

    @dlt.source
    def some_source():
        return [static_data(), dynamic_func_data(), dynamic_mark_data(), nested_data()]

    source = some_source()
    p = dlt.pipeline(pipeline_name=uniq_id(), destination=DUMMY_COMPLETE)
    p.run(source)

    schema = p.default_schema

    assert schema.tables["some_table"]["resource"] == "static_data"
    assert schema.tables["dynamic_func_table"]["resource"] == "dynamic_func_data"
    assert schema.tables["dynamic_mark_table"]["resource"] == "dynamic_mark_data"
    assert schema.tables["parent_table"]["resource"] == "nested_data"
    assert "resource" not in schema.tables["parent_table__items"]


def test_preserve_fields_order() -> None:
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")

    item = {"col_1": 1, "col_2": 2, "col_3": "list"}
    p.extract([item], table_name="order_1")
    p.normalize()

    @dlt.resource(name="order_2")
    def ordered_dict():
        yield {"col_1": 1, "col_2": 2, "col_3": "list"}

    def reverse_order(item):
        rev_dict = {}
        for k in reversed(item.keys()):
            rev_dict[k] = item[k]
        return rev_dict

    p.extract(ordered_dict().add_map(reverse_order))
    p.normalize()

    assert list(p.default_schema.tables["order_1"]["columns"].keys()) == [
        "col_1",
        "col_2",
        "col_3",
        "_dlt_load_id",
        "_dlt_id",
    ]
    assert list(p.default_schema.tables["order_2"]["columns"].keys()) == [
        "col_3",
        "col_2",
        "col_1",
        "_dlt_load_id",
        "_dlt_id",
    ]


def test_preserve_new_fields_order_on_append() -> None:
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")

    item = {"c1": 1, "c2": 2, "c3": "list"}
    p.extract([item], table_name="order_1")
    p.normalize()
    assert list(p.default_schema.get_table_columns("order_1").keys()) == [
        "c1",
        "c2",
        "c3",
        "_dlt_load_id",
        "_dlt_id",
    ]

    # add columns
    item = {"c1": 1, "c4": 2.0, "c3": "list", "c5": {"x": 1}}
    p.extract([item], table_name="order_1")
    p.normalize()
    assert list(p.default_schema.get_table_columns("order_1").keys()) == [
        "c1",
        "c2",
        "c3",
        "_dlt_load_id",
        "_dlt_id",
        "c4",
        "c5__x",
    ]


def test_preserve_fields_order_incomplete_columns() -> None:
    p = dlt.pipeline(pipeline_name="column_order", destination="dummy")
    # incomplete columns (without data type) will be added in order of fields in data

    @dlt.resource(columns={"c3": {"precision": 32}}, primary_key="c2")
    def items():
        yield {"c1": 1, "c2": 1, "c3": 1}

    p.extract(items)
    p.normalize()
    assert list(p.default_schema.get_table_columns("items").keys()) == [
        "c1",
        "c2",
        "c3",
        "_dlt_load_id",
        "_dlt_id",
    ]

    # complete columns preserve order in "columns"
    p = p.drop()

    @dlt.resource(columns={"c3": {"precision": 32, "data_type": "decimal"}}, primary_key="c1")
    def items2():
        yield {"c1": 1, "c2": 1, "c3": 1}

    p.extract(items2)
    p.normalize()
    # c3 was first so goes first
    assert list(p.default_schema.get_table_columns("items2").keys()) == [
        "c3",
        "c1",
        "c2",
        "_dlt_load_id",
        "_dlt_id",
    ]


def test_pipeline_log_progress() -> None:
    os.environ["TIMEOUT"] = "3.0"

    # will attach dlt logger
    p = dlt.pipeline(
        destination="dummy", progress=dlt.progress.log(0.5, logger=None, log_level=logging.WARNING)
    )
    # collector was created before pipeline so logger is not attached
    assert cast(LogCollector, p.collector).logger is None
    p.extract(many_delayed(2, 10))
    # dlt logger attached
    assert cast(LogCollector, p.collector).logger is not None

    # pass explicit root logger
    p = dlt.attach(progress=dlt.progress.log(0.5, logger=logging.getLogger()))
    assert cast(LogCollector, p.collector).logger is not None
    p.extract(many_delayed(2, 10))


def test_pipeline_source_state_activation() -> None:
    appendix_yielded = None

    @dlt.source
    def reads_state(source_st, resource_st):
        if dlt.current.source_state().get("appendix"):

            @dlt.resource
            def appendix():
                nonlocal appendix_yielded
                appendix_yielded = dlt.current.source_state().get("appendix") or ["NO"]
                yield appendix_yielded

            yield appendix

        @dlt.resource
        def writes_state():
            dlt.current.source_state()["appendix"] = source_st
            dlt.current.resource_state()["RX"] = resource_st
            yield from [1, 2, 3]

        yield writes_state

    # activate first pipeline
    p_appendix = dlt.pipeline(pipeline_name="appendix_p")
    assert p_appendix.is_active
    s_appendix = reads_state("appendix", "r_appendix")
    assert s_appendix.state == {}
    # create state by running extract
    p_appendix.extract(s_appendix)
    assert s_appendix.state == {
        "appendix": "appendix",
        "resources": {"writes_state": {"RX": "r_appendix"}},
    }
    assert s_appendix.writes_state.state == {"RX": "r_appendix"}

    # change the active pipeline
    p_postfix = dlt.pipeline(pipeline_name="postfix_p")
    # now state comes from active pipeline which has an empty state
    assert s_appendix.state == {}
    # and back
    p_appendix.activate()
    assert s_appendix.writes_state.state == {"RX": "r_appendix"}

    # create another source
    s_w_appendix = reads_state("appendix", "r_appendix")
    # has appendix because state is present
    assert hasattr(s_w_appendix, "appendix")

    # now extract the source in p_postfix that does not have a state
    p_postfix.activate()
    p_postfix.extract(s_w_appendix)
    # so appendix yielded NO
    # NOTE: this behavior is not intuitive, s_w_appendix was created with p_appendix active so appendix resource was created because state was set
    # but we evaluated it in p_postfix pipeline without the state
    assert appendix_yielded == ["NO"]

    with pytest.raises(PipelineNotActive):
        p_appendix.deactivate()

    p_postfix.deactivate()
    with pytest.raises(PipelineStateNotAvailable):
        assert s_appendix.state == {}


def test_extract_add_tables() -> None:
    # we extract and make sure that tables are added to schema
    s = airtable_emojis()
    assert list(s.resources.keys()) == ["💰Budget", "📆 Schedule", "🦚Peacock", "🦚WidePeacock"]
    assert s.resources["🦚Peacock"].compute_table_schema()["resource"] == "🦚Peacock"
    # only name will be normalized
    assert s.resources["🦚Peacock"].compute_table_schema()["name"] == "🦚Peacock"
    assert (
        s.resources["💰Budget"].compute_table_schema()["columns"]["🔑book_id"]["name"]
        == "🔑book_id"
    )
    pipeline = dlt.pipeline(pipeline_name="emojis", destination="dummy")
    info = pipeline.extract(s)
    assert info.extract_data_info[0]["name"] == "airtable_emojis"
    schema = pipeline.default_schema
    assert schema.tables["_schedule"]["resource"] == "📆 Schedule"
    assert len(schema.tables["_schedule"]["columns"]) == 0
    assert "_budget" not in schema.tables

    # extract peacock
    s = airtable_emojis()
    s.resources["🦚Peacock"].selected = True
    pipeline.extract(s)
    # live schema
    assert schema.tables["_peacock"]["resource"] == "🦚Peacock"
    assert len(schema.tables["_peacock"]["columns"]) == 1
    assert "_wide_peacock" not in schema.tables

    # extract wide peacock
    s = airtable_emojis()
    s.resources["🦚WidePeacock"].selected = True
    pipeline.extract(s)
    assert len(schema.tables["_wide_peacock"]["columns"]) == 0
    assert schema.tables["_wide_peacock"]["resource"] == "🦚WidePeacock"

    # now normalize, we should have columns
    pipeline.normalize()
    # dlt tables added
    assert len(schema.tables["_wide_peacock"]["columns"]) == 2
    # resource still not normalized
    assert schema.tables["_wide_peacock"]["resource"] == "🦚WidePeacock"

    # reload schema
    schema = pipeline._schema_storage.load_schema("airtable_emojis")
    assert len(schema.tables["_wide_peacock"]["columns"]) == 2
    # resource still not normalized
    assert schema.tables["_wide_peacock"]["resource"] == "🦚WidePeacock"


def test_emojis_resource_names() -> None:
    pipeline = dlt.pipeline(pipeline_name="emojis", destination="duckdb")
    info = pipeline.run(airtable_emojis())
    assert_load_info(info)
    # make sure that resource in schema update has a right name
    table = info.load_packages[0].schema_update["_schedule"]
    assert table["resource"] == "📆 Schedule"
    # only schedule is added
    assert set(info.load_packages[0].schema_update.keys()) == {
        "_dlt_version",
        "_dlt_loads",
        "_schedule",
        "_dlt_pipeline_state",
    }
    info = pipeline.run(airtable_emojis())
    assert_load_info(info)
    # here we add _peacock with has primary_key (so at least single column)
    s = airtable_emojis()
    s.resources["🦚Peacock"].selected = True
    info = pipeline.run(s)
    assert_load_info(info)
    table = info.load_packages[0].schema_update["_peacock"]
    assert table["resource"] == "🦚Peacock"
    # here we add _wide_peacock which has no columns
    s = airtable_emojis()
    s.resources["🦚WidePeacock"].selected = True
    info = pipeline.run(s)
    assert_load_info(info)
    table = info.load_packages[0].schema_update["_wide_peacock"]
    assert table["resource"] == "🦚WidePeacock"


def test_apply_hints_infer_hints() -> None:
    @dlt.source
    def infer():
        yield dlt.resource(
            [{"id": 1, "timestamp": "NOW"}],
            name="table1",
            columns=[new_column("timestamp", nullable=True)],
        )

    new_new_hints = {"not_null": ["timestamp"], "primary_key": ["id"]}
    s = infer()
    s.schema.merge_hints(new_new_hints)  # type: ignore[arg-type]
    pipeline = dlt.pipeline(pipeline_name="inf", destination=DUMMY_COMPLETE)
    pipeline.run(s)
    # check schema
    table = pipeline.default_schema.get_table("table1")
    # nullable True coming from hint overrides inferred hint
    assert table["columns"]["timestamp"] == {
        "name": "timestamp",
        "data_type": "text",
        "nullable": True,
    }
    # fully from data
    assert table["columns"]["id"] == {
        "name": "id",
        "data_type": "bigint",
        "nullable": True,
        "primary_key": True,
    }

    # remove primary key and change nullable
    s = infer()
    s.table1.apply_hints(
        columns=[
            {"name": "timestamp", "nullable": False},
            {"name": "id", "nullable": False, "primary_key": False},
        ]
    )
    pipeline.run(s)
    table = pipeline.default_schema.get_table("table1")
    # hints overwrite pipeline schema
    assert table["columns"]["timestamp"] == {
        "name": "timestamp",
        "data_type": "text",
        "nullable": False,
    }
    assert table["columns"]["id"] == {
        "name": "id",
        "data_type": "bigint",
        "nullable": False,
        "primary_key": False,
    }
    # print(pipeline.default_schema.to_pretty_yaml())


def test_invalid_data_edge_cases() -> None:
    # pass not evaluated source function
    @dlt.source
    def my_source():
        return dlt.resource(itertools.count(start=1), name="infinity").add_limit(5)

    pipeline = dlt.pipeline(pipeline_name="invalid", destination=DUMMY_COMPLETE)
    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(my_source)
    assert isinstance(pip_ex.value.__context__, PipeGenInvalid)
    assert "dlt.source" in str(pip_ex.value)

    def res_return():
        return dlt.resource(itertools.count(start=1), name="infinity").add_limit(5)

    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(res_return)
    assert isinstance(pip_ex.value.__context__, PipeGenInvalid)
    assert "dlt.resource" in str(pip_ex.value)

    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run({"a": "b"}, table_name="data")
    assert isinstance(pip_ex.value.__context__, InvalidResourceDataTypeBasic)

    # check same cases but that yield
    @dlt.source
    def my_source_yield():
        yield dlt.resource(itertools.count(start=1), name="infinity").add_limit(5)

    pipeline = dlt.pipeline(pipeline_name="invalid", destination=DUMMY_COMPLETE)
    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(my_source_yield)
    assert isinstance(pip_ex.value.__context__, PipeGenInvalid)
    assert "dlt.source" in str(pip_ex.value)

    def res_return_yield():
        return dlt.resource(itertools.count(start=1), name="infinity").add_limit(5)

    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(res_return_yield)
    assert isinstance(pip_ex.value.__context__, PipeGenInvalid)
    assert "dlt.resource" in str(pip_ex.value)


def test_resource_rename_same_table():
    @dlt.resource(write_disposition="replace")
    def generic(start):
        dlt.current.resource_state()["start"] = start
        yield [{"id": idx, "text": "A" * idx} for idx in range(start, start + 10)]

    pipeline = dlt.pipeline(destination="duckdb")
    load_info = pipeline.run(
        [generic(10).with_name("state1"), generic(20).with_name("state2")],
        table_name="single_table",
    )
    assert_load_info(load_info)
    # both resources loaded
    assert pipeline.last_trace.last_normalize_info.row_counts["single_table"] == 20
    # only this table and state
    assert len(pipeline.last_trace.last_normalize_info.row_counts) == 2

    # check state
    # state1 should have 10
    assert generic(0).with_name("state1").state["start"] == 10
    # state2 is 10
    assert generic(0).with_name("state2").state["start"] == 20

    # NOTE: only one resource will be set in table
    assert pipeline.default_schema.get_table("single_table")["resource"] == "state1"

    # now load only state1
    load_info = pipeline.run(
        [
            generic(5).with_name("state1"),
        ],
        table_name="single_table",
    )
    assert_load_info(load_info)
    # both resources loaded
    assert pipeline.last_trace.last_normalize_info.row_counts["single_table"] == 10
    assert generic(0).with_name("state1").state["start"] == 5
    # resource got swapped to the most recent one
    assert pipeline.default_schema.get_table("single_table")["resource"] == "state1"


def test_drop_with_new_name() -> None:
    old_test_name = "old_pipeline_name"
    new_test_name = "new_pipeline_name"

    pipeline = dlt.pipeline(pipeline_name=old_test_name, destination="duckdb")
    new_pipeline = pipeline.drop(pipeline_name=new_test_name)

    assert new_pipeline.pipeline_name == new_test_name


def test_schema_version_increase_and_source_update() -> None:
    now = pendulum.now()

    @dlt.source
    def autodetect():
        # add unix ts autodetection to current source schema
        dlt.current.source_schema().add_type_detection("timestamp")
        return dlt.resource(
            [int(now.timestamp()), int(now.timestamp() + 1), int(now.timestamp() + 2)],
            name="numbers",
        )

    pipeline = dlt.pipeline(destination="duckdb")
    # control version of the schema
    auto_source = autodetect()
    assert auto_source.schema.stored_version is None
    pipeline.extract(auto_source)
    # extract did a first save
    assert pipeline.default_schema.stored_version == 1
    # only one prev hash
    assert len(pipeline.default_schema.previous_hashes) == 1
    # source schema was updated in the pipeline
    assert auto_source.schema.stored_version == 1
    # source has pipeline schema
    assert pipeline.default_schema is auto_source.schema

    pipeline.normalize()
    # columns added and schema was saved in between
    assert pipeline.default_schema.stored_version == 2
    assert len(pipeline.default_schema.previous_hashes) == 2
    # source schema still updated
    assert auto_source.schema.stored_version == 2
    assert pipeline.default_schema is auto_source.schema
    pipeline.load()
    # nothing changed in load
    assert pipeline.default_schema.stored_version == 2
    assert pipeline.default_schema is auto_source.schema

    # run same source again
    pipeline.extract(auto_source)
    assert pipeline.default_schema.stored_version == 2
    assert pipeline.default_schema is auto_source.schema
    pipeline.normalize()
    assert pipeline.default_schema.stored_version == 2
    pipeline.load()
    assert pipeline.default_schema.stored_version == 2

    # run another instance of the same source
    pipeline.run(autodetect())
    assert pipeline.default_schema.stored_version == 2
    assert pipeline.default_schema is auto_source.schema
    assert "timestamp" in pipeline.default_schema.settings["detections"]

    # data has compatible schema with "numbers" but schema is taken from pipeline
    pipeline.run([1, 2, 3], table_name="numbers")
    assert "timestamp" in pipeline.default_schema.settings["detections"]
    assert pipeline.default_schema.stored_version == 2
    assert pipeline.default_schema is auto_source.schema

    # new table will evolve schema
    pipeline.run([1, 2, 3], table_name="seq")
    assert "timestamp" in pipeline.default_schema.settings["detections"]
    assert pipeline.default_schema.stored_version == 4
    assert pipeline.default_schema is auto_source.schema


def test_remove_autodetect() -> None:
    now = pendulum.now()

    @dlt.source
    def autodetect():
        # add unix ts autodetection to current source schema
        dlt.current.source_schema().add_type_detection("timestamp")
        return dlt.resource(
            [int(now.timestamp()), int(now.timestamp() + 1), int(now.timestamp() + 2)],
            name="numbers",
        )

    pipeline = dlt.pipeline(destination="duckdb")
    auto_source = autodetect()
    pipeline.extract(auto_source)
    pipeline.normalize()

    # unix ts recognized
    assert (
        pipeline.default_schema.get_table("numbers")["columns"]["value"]["data_type"] == "timestamp"
    )
    pipeline.load()

    pipeline = pipeline.drop()

    source = autodetect()
    assert "timestamp" in source.schema.settings["detections"]
    source.schema.remove_type_detection("timestamp")
    assert "timestamp" not in source.schema.settings["detections"]

    pipeline = dlt.pipeline(destination="duckdb")
    pipeline.run(source)
    assert "timestamp" not in pipeline.default_schema.settings["detections"]

    assert pipeline.default_schema.get_table("numbers")["columns"]["value"]["data_type"] == "bigint"


def test_flattened_column_hint() -> None:
    now = pendulum.now()

    # @dlt.resource(columns=[{"name": "value__timestamp", "data_type": "timestamp"}])
    @dlt.resource()
    def flattened_dict():
        # dlt.current.source_schema().add_type_detection("timestamp")

        for delta in range(4):
            yield {
                "delta": delta,
                "values": [{"Value": {"timestampValue": now.timestamp() + delta}}],
            }

    @dlt.source
    def nested_resource():
        # we need to create a whole structure
        dict_resource = flattened_dict()
        # add table from resource
        dlt.current.source_schema().update_table(dict_resource.compute_table_schema())
        values_table = new_table(
            dict_resource.name + "__values",
            parent_table_name=dict_resource.name,
            columns=[{"name": "value__timestamp_value", "data_type": "timestamp"}],
        )
        # and child table
        dlt.current.source_schema().update_table(values_table)
        return dict_resource

    pipeline = dlt.pipeline(destination="duckdb")
    pipeline.run(nested_resource())
    # print(pipeline.default_schema.to_pretty_yaml())
    assert (
        pipeline.default_schema.get_table("flattened_dict__values")["columns"][
            "value__timestamp_value"
        ]["data_type"]
        == "timestamp"
    )
    # make sure data is there
    assert pipeline.last_trace.last_normalize_info.row_counts["flattened_dict__values"] == 4


def test_empty_rows_are_included() -> None:
    """Empty rows where all values are `None` or empty dicts
    create rows in the dataset with `NULL` in all columns
    """
    pipeline = dlt.pipeline(destination="duckdb")

    pipeline.run(iter([{}, {}, {}]), table_name="empty_rows")
    pipeline.run(iter([{"a": 1}, {}, {}]), table_name="empty_rows")
    pipeline.run(iter([{"a": None}, {}]), table_name="empty_rows")

    with pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT a FROM empty_rows ORDER BY a")

    values = [r[0] for r in rows]
    assert values == [1, None, None, None, None, None, None, None]


def test_resource_state_name_not_normalized() -> None:
    pipeline = dlt.pipeline(pipeline_name="emojis", destination="duckdb")
    peacock_s = airtable_emojis().with_resources("🦚Peacock")
    pipeline.extract(peacock_s)
    assert peacock_s.resources["🦚Peacock"].state == {"🦚🦚🦚": "🦚"}
    pipeline.normalize()
    pipeline.load()

    # get state from destination
    from dlt.pipeline.state_sync import load_pipeline_state_from_destination

    client: WithStateSync
    with pipeline.destination_client() as client:  # type: ignore[assignment]
        state = load_pipeline_state_from_destination(pipeline.pipeline_name, client)
        assert "airtable_emojis" in state["sources"]
        assert state["sources"]["airtable_emojis"]["resources"] == {"🦚Peacock": {"🦚🦚🦚": "🦚"}}


def test_pipeline_list_packages() -> None:
    pipeline = dlt.pipeline(pipeline_name="emojis", destination="dummy")
    pipeline.extract(airtable_emojis())
    load_ids = pipeline.list_extracted_load_packages()
    assert len(load_ids) == 1
    # two new packages: for emojis schema and emojis_2
    pipeline.extract(
        [airtable_emojis(), airtable_emojis(), airtable_emojis().clone(with_name="emojis_2")]
    )
    load_ids = pipeline.list_extracted_load_packages()
    assert len(load_ids) == 3
    extracted_package = pipeline.get_load_package_info(load_ids[1])
    assert extracted_package.schema_name == "airtable_emojis"
    extracted_package = pipeline.get_load_package_info(load_ids[2])
    assert extracted_package.schema_name == "emojis_2"
    extracted_package = pipeline.get_load_package_info(load_ids[0])
    assert extracted_package.state == "extracted"
    assert extracted_package.schema_name == "airtable_emojis"
    # same load id continues till the end
    pipeline.normalize()
    load_ids_n = pipeline.list_normalized_load_packages()
    assert load_ids == load_ids_n
    normalized_package = pipeline.get_load_package_info(load_ids[0])
    # same number of new jobs
    assert normalized_package.state == "normalized"
    assert len(normalized_package.jobs["new_jobs"]) == len(extracted_package.jobs["new_jobs"])
    # load all 3 packages and fail all jobs in them
    os.environ["RAISE_ON_FAILED_JOBS"] = "false"  # do not raise, complete package till the end
    os.environ["FAIL_PROB"] = "1.0"
    pipeline.load()
    load_ids_l = pipeline.list_completed_load_packages()
    assert load_ids == load_ids_l
    loaded_package = pipeline.get_load_package_info(load_ids[0])
    assert len(loaded_package.jobs["failed_jobs"]) == len(extracted_package.jobs["new_jobs"])
    assert loaded_package.state == "loaded"
    failed_jobs = pipeline.list_failed_jobs_in_package(load_ids[0])
    assert len(loaded_package.jobs["failed_jobs"]) == len(failed_jobs)


def test_remove_pending_packages() -> None:
    pipeline = dlt.pipeline(pipeline_name="emojis", destination=DUMMY_COMPLETE)
    pipeline.extract(airtable_emojis())
    assert pipeline.has_pending_data
    pipeline.drop_pending_packages()
    assert pipeline.has_pending_data is False
    pipeline.extract(airtable_emojis())
    pipeline.normalize()
    pipeline.extract(airtable_emojis())
    assert pipeline.has_pending_data
    pipeline.drop_pending_packages()
    assert pipeline.has_pending_data is False
    # partial load
    os.environ["EXCEPTION_PROB"] = "1.0"
    os.environ["TIMEOUT"] = "1.0"
    # will make job go into retry state
    with pytest.raises(PipelineStepFailed):
        pipeline.run(airtable_emojis())
    # move job into completed folder manually to simulate partial package
    load_storage = pipeline._get_load_storage()
    load_id = load_storage.normalized_packages.list_packages()[0]
    job = load_storage.normalized_packages.list_new_jobs(load_id)[0]
    started_path = load_storage.normalized_packages.start_job(
        load_id, FileStorage.get_file_name_from_file_path(job)
    )
    completed_path = load_storage.normalized_packages.complete_job(
        load_id, FileStorage.get_file_name_from_file_path(job)
    )
    # to test partial loads we need two jobs one completed an one in another state
    # to simulate this, we just duplicate the completed job into the started path
    shutil.copyfile(completed_path, started_path)
    # now "with partial loads" can be tested
    assert pipeline.has_pending_data
    pipeline.drop_pending_packages(with_partial_loads=False)
    assert pipeline.has_pending_data
    pipeline.drop_pending_packages()
    assert pipeline.has_pending_data is False


@pytest.mark.parametrize("workers", (1, 4), ids=("1 norm worker", "4 norm workers"))
def test_parallel_pipelines_threads(workers: int) -> None:
    # critical section to control pipeline steps
    init_lock = threading.Lock()
    extract_ev = threading.Event()
    normalize_ev = threading.Event()
    load_ev = threading.Event()
    # control main thread
    sem = threading.Semaphore(0)

    # rotate the files frequently so we have parallel normalize and load
    os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "10"
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "10"

    # force spawn process pool
    os.environ["NORMALIZE__START_METHOD"] = "spawn"

    page_repeats = 1

    # set the extra per pipeline
    os.environ["PIPELINE_1__EXTRA"] = "CFG_P_1"
    os.environ["PIPELINE_2__EXTRA"] = "CFG_P_2"

    def _run_pipeline(pipeline_name: str) -> Tuple[LoadInfo, PipelineContext, DictStrAny]:
        try:

            @dlt.transformer(
                name="github_repo_events",
                write_disposition="append",
                table_name=lambda i: i["type"],
            )
            def github_repo_events(page, extra):
                # test setting the resource state
                dlt.current.resource_state()["extra"] = extra
                yield page

            @dlt.transformer
            async def slow(items):
                await asyncio.sleep(0.1)
                return items

            @dlt.transformer
            @dlt.defer
            def slow_func(items, extra):
                # sdd configurable extra to each element
                sleep(0.1)
                return map(lambda item: {**item, **{"extra": extra}}, items)

            @dlt.source
            def github(extra: str = dlt.config.value):
                # generate github events, push them through futures and thread pools and then dispatch to separate tables
                return (
                    _get_shuffled_events(repeat=page_repeats)
                    | slow
                    | slow_func(extra)
                    | github_repo_events(extra)
                )

            # make sure that only one pipeline is created
            with init_lock:
                pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="duckdb")
                context = Container()[PipelineContext]
        finally:
            sem.release()
        # start every step at the same moment to increase chances of any race conditions to happen
        extract_ev.wait()
        context_2 = Container()[PipelineContext]
        try:
            pipeline.extract(github())
        finally:
            sem.release()
        normalize_ev.wait()
        try:
            pipeline.normalize(workers=workers)
        finally:
            sem.release()
        load_ev.wait()
        info = pipeline.load()

        # get counts in the thread
        counts = load_data_table_counts(pipeline)

        assert context is context_2
        return info, context, counts

    with ThreadPoolExecutor(max_workers=4) as pool:
        f_1 = pool.submit(_run_pipeline, "pipeline_1")
        f_2 = pool.submit(_run_pipeline, "pipeline_2")

        sem.acquire()
        sem.acquire()
        if f_1.done():
            raise f_1.exception()
        if f_2.done():
            raise f_2.exception()
        extract_ev.set()
        sem.acquire()
        sem.acquire()
        if f_1.done():
            raise f_1.exception()
        if f_2.done():
            raise f_2.exception()
        normalize_ev.set()
        sem.acquire()
        sem.acquire()
        if f_1.done():
            raise f_1.exception()
        if f_2.done():
            raise f_2.exception()
        load_ev.set()

        info_1, context_1, counts_1 = f_1.result()
        info_2, context_2, counts_2 = f_2.result()

    assert_load_info(info_1)
    assert_load_info(info_2)

    pipeline_1: dlt.Pipeline = context_1.pipeline()  # type: ignore
    pipeline_2: dlt.Pipeline = context_2.pipeline()  # type: ignore

    n_counts_1 = pipeline_1.last_trace.last_normalize_info
    assert n_counts_1.row_counts["push_event"] == 8 * page_repeats == counts_1["push_event"]
    n_counts_2 = pipeline_2.last_trace.last_normalize_info
    assert n_counts_2.row_counts["push_event"] == 8 * page_repeats == counts_2["push_event"]

    assert pipeline_1.pipeline_name == "pipeline_1"
    assert pipeline_2.pipeline_name == "pipeline_2"

    # check if resource state has extra
    assert pipeline_1.state["sources"]["github"]["resources"]["github_repo_events"] == {
        "extra": "CFG_P_1"
    }
    assert pipeline_2.state["sources"]["github"]["resources"]["github_repo_events"] == {
        "extra": "CFG_P_2"
    }

    # make sure we can still access data
    pipeline_1.activate()  # activate pipeline to access inner duckdb
    assert load_data_table_counts(pipeline_1) == counts_1
    pipeline_2.activate()
    assert load_data_table_counts(pipeline_2) == counts_2


@pytest.mark.parametrize("workers", (1, 4), ids=("1 norm worker", "4 norm workers"))
def test_parallel_pipelines_async(workers: int) -> None:
    os.environ["NORMALIZE__WORKERS"] = str(workers)

    # create both futures and thread parallel resources

    def async_table():
        async def _gen(idx):
            await asyncio.sleep(0.1)
            return {"async_gen": idx}

        # just yield futures in a loop
        for idx_ in range(10):
            yield _gen(idx_)

    def defer_table():
        @dlt.defer
        def _gen(idx):
            sleep(0.1)
            return {"thread_gen": idx}

        # just yield futures in a loop
        for idx_ in range(5):
            yield _gen(idx_)

    def _run_pipeline(pipeline, gen_) -> LoadInfo:
        # run the pipeline in a thread, also instantiate generators here!
        # Python does not let you use generators across instances
        return pipeline.run(gen_())

    # declare pipelines in main thread then run them "async"
    pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", dev_mode=True)
    pipeline_2 = dlt.pipeline("pipeline_2", destination="duckdb", dev_mode=True)

    async def _run_async():
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as executor:
            results = await asyncio.gather(
                loop.run_in_executor(executor, _run_pipeline, pipeline_1, async_table),
                loop.run_in_executor(executor, _run_pipeline, pipeline_2, defer_table),
            )
        assert_load_info(results[0])
        assert_load_info(results[1])

    asyncio.run(_run_async())
    pipeline_1.activate()  # activate pipeline 1 to access inner duckdb
    assert load_data_table_counts(pipeline_1) == {"async_table": 10}
    pipeline_2.activate()  # activate pipeline 2 to access inner duckdb
    assert load_data_table_counts(pipeline_2) == {"defer_table": 5}


def test_resource_while_stop() -> None:
    def product():
        stop = False

        @dlt.defer
        def get_page(page_num):
            nonlocal stop

            # print(f"Fetching page {page_num}")
            items = api_fetch(page_num)
            # print(f"Retrieved {len(items)} from page {page_num}")

            if len(items) == 0:
                stop = True
            return items

        idx = 0
        while not stop:
            yield get_page(idx)
            idx += 1

    def api_fetch(page_num):
        import time

        time.sleep(1)
        if page_num < 12:
            return [{"id": page_num}]
        else:
            return []

    pipeline = dlt.pipeline("pipeline_1", destination="duckdb", dev_mode=True)
    load_info = pipeline.run(product())
    assert_load_info(load_info)
    assert pipeline.last_trace.last_normalize_info.row_counts["product"] == 12


def test_run_with_pua_payload() -> None:
    # prepare some data and complete load with run
    os.environ["COMPLETED_PROB"] = "1.0"
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="duckdb")
    print(pipeline_name)
    from dlt.common.json import PUA_START, PUA_CHARACTER_MAX

    def some_data():
        yield from [
            # text is only PUA
            {"id": 1, "text": chr(PUA_START)},
            {"id": 2, "text": chr(PUA_START - 1)},
            {"id": 3, "text": chr(PUA_START + 1)},
            {"id": 4, "text": chr(PUA_START + PUA_CHARACTER_MAX + 1)},
            # PUA inside text
            {"id": 5, "text": f"a{chr(PUA_START)}b"},
            {"id": 6, "text": f"a{chr(PUA_START - 1)}b"},
            {"id": 7, "text": f"a{chr(PUA_START + 1)}b"},
            # text starts with PUA
            {"id": 8, "text": f"{chr(PUA_START)}a"},
            {"id": 9, "text": f"{chr(PUA_START - 1)}a"},
            {"id": 10, "text": f"{chr(PUA_START + 1)}a"},
        ]

    @dlt.source
    def source():
        return dlt.resource(some_data(), name="pua_data")

    load_info = p.run(source())
    assert p.last_trace.last_normalize_info.row_counts["pua_data"] == 10

    with p.sql_client() as client:
        rows = client.execute_sql("SELECT text FROM pua_data ORDER BY id")

    values = [r[0] for r in rows]
    assert values == [
        "\uf026",
        "\uf025",
        "\uf027",
        "\uf02f",
        "a\uf026b",
        "a\uf025b",
        "a\uf027b",
        "\uf026a",
        "\uf025a",
        "\uf027a",
    ]
    assert len(load_info.loads_ids) == 1


def test_pipeline_load_info_metrics_schema_is_not_changing() -> None:
    """Test if load info schema is idempotent throughout multiple load cycles

    ## Setup

    We will run the same pipeline with

        1. A single source returning one resource and collect `schema.version_hash`,
        2. Another source returning 2 resources with more complex data and collect `schema.version_hash`,
        3. At last we run both sources,
        4. For each 1. 2. 3. we load `last_extract_info`, `last_normalize_info` and `last_load_info` and collect `schema.version_hash`

    ## Expected

    `version_hash` collected in each stage should remain the same at all times.
    """
    data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]

    # this source must have all the hints so other sources do not change trace schema (extract/hints)

    @dlt.source
    def users_source():
        return dlt.resource([data], name="users_resource")

    @dlt.source
    def taxi_demand_source():
        @dlt.resource(
            primary_key="city", columns=[{"name": "id", "data_type": "bigint", "precision": 4}]
        )
        def locations(idx=dlt.sources.incremental("id")):
            for idx in range(10):
                yield {
                    "id": idx,
                    "address": f"address-{idx}",
                    "city": f"city-{idx}",
                }

        @dlt.resource(primary_key="id")
        def demand_map():
            for idx in range(10):
                yield {
                    "id": idx,
                    "city": f"city-{idx}",
                    "demand": random.randint(0, 10000),
                }

        return [locations, demand_map]

    schema = dlt.Schema(name="nice_load_info_schema")
    pipeline = dlt.pipeline(
        pipeline_name="quick_start",
        destination="duckdb",
        dataset_name="mydata",
    )

    taxi_load_info = pipeline.run(
        taxi_demand_source(),
    )

    schema_hashset = set()
    pipeline.run(
        [taxi_load_info],
        table_name="_load_info",
        schema=schema,
    )

    pipeline.run(
        [pipeline.last_trace.last_normalize_info],
        table_name="_normalize_info",
        schema=schema,
    )

    pipeline.run(
        [pipeline.last_trace.last_extract_info],
        table_name="_extract_info",
        schema=schema,
    )
    schema_hashset.add(pipeline.schemas["nice_load_info_schema"].version_hash)
    trace_schema = pipeline.schemas["nice_load_info_schema"].to_pretty_yaml()

    users_load_info = pipeline.run(
        users_source(),
    )

    pipeline.run(
        [users_load_info],
        table_name="_load_info",
        schema=schema,
    )
    assert trace_schema == pipeline.schemas["nice_load_info_schema"].to_pretty_yaml()
    schema_hashset.add(pipeline.schemas["nice_load_info_schema"].version_hash)
    assert len(schema_hashset) == 1

    pipeline.run(
        [pipeline.last_trace.last_normalize_info],
        table_name="_normalize_info",
        schema=schema,
    )
    schema_hashset.add(pipeline.schemas["nice_load_info_schema"].version_hash)
    assert len(schema_hashset) == 1

    pipeline.run(
        [pipeline.last_trace.last_extract_info],
        table_name="_extract_info",
        schema=schema,
    )
    schema_hashset.add(pipeline.schemas["nice_load_info_schema"].version_hash)
    assert len(schema_hashset) == 1

    load_info = pipeline.run(
        [users_source(), taxi_demand_source()],
    )

    pipeline.run(
        [load_info],
        table_name="_load_info",
        schema=schema,
    )

    schema_hashset.add(pipeline.schemas["nice_load_info_schema"].version_hash)

    pipeline.run(
        [pipeline.last_trace.last_normalize_info],
        table_name="_normalize_info",
        schema=schema,
    )

    schema_hashset.add(pipeline.schemas["nice_load_info_schema"].version_hash)

    pipeline.run(
        [pipeline.last_trace.last_extract_info],
        table_name="_extract_info",
        schema=schema,
    )

    schema_hashset.add(pipeline.schemas["nice_load_info_schema"].version_hash)

    assert len(schema_hashset) == 1


def test_yielding_empty_list_creates_table() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="empty_start",
        destination="duckdb",
        dataset_name="mydata",
    )

    # empty list should create empty table in the destination but with the required schema
    extract_info = pipeline.extract(
        [MaterializedEmptyList()],
        table_name="empty",
        columns=[{"name": "id", "data_type": "bigint", "nullable": True}],
    )
    print(extract_info)
    normalize_info = pipeline.normalize()
    print(normalize_info)
    assert normalize_info.row_counts["empty"] == 0
    load_info = pipeline.load()
    # print(load_info.asstr(verbosity=3))
    assert_load_info(load_info)
    assert_data_table_counts(pipeline, {"empty": 0})
    # make sure we have expected columns
    assert set(pipeline.default_schema.tables["empty"]["columns"].keys()) == {
        "id",
        "_dlt_load_id",
        "_dlt_id",
    }

    # load some data
    pipeline.run([{"id": 1}], table_name="empty")
    assert_data_table_counts(pipeline, {"empty": 1})

    # update schema on existing table
    pipeline.run(
        [MaterializedEmptyList()],
        table_name="empty",
        columns=[{"name": "user_name", "data_type": "text", "nullable": True}],
    )
    assert_data_table_counts(pipeline, {"empty": 1})
    assert set(pipeline.default_schema.tables["empty"]["columns"].keys()) == {
        "id",
        "_dlt_load_id",
        "_dlt_id",
        "user_name",
    }
    with pipeline.sql_client() as client:
        with client.execute_query("SELECT id, user_name FROM empty") as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 1
            assert rows[0] == (1, None)


local_paths = [os.path.abspath("_storage"), "_storage"]
if os.name == "nt":
    local_paths += [
        # UNC extended path
        "\\\\?\\UNC\\localhost\\" + os.path.abspath("_storage").replace(":", "$"),
        # UNC path
        "\\\\localhost\\" + os.path.abspath("_storage").replace(":", "$"),
    ]


@pytest.mark.parametrize("local_path", local_paths)
def test_local_filesystem_destination(local_path: str) -> None:
    dataset_name = "mydata_" + uniq_id()

    @dlt.resource
    def stateful_resource():
        dlt.current.source_state()["mark"] = 1
        yield [1, 2, 3]

    pipeline = dlt.pipeline(
        pipeline_name="local_files",
        destination=dlt.destinations.filesystem(local_path),
        dataset_name=dataset_name,
    )
    info = pipeline.run(stateful_resource(), table_name="numbers", write_disposition="replace")
    assert_load_info(info)

    info = pipeline.run(stateful_resource(), table_name="numbers", write_disposition="replace")
    assert_load_info(info)

    pipeline = pipeline.drop()

    # must be able to restore the schema and state
    pipeline.sync_destination()
    assert pipeline.state["sources"]["local_files"]["mark"] == 1
    assert pipeline.default_schema.get_table("numbers") is not None

    # check all the files, paths may get messed up in many different ways
    # and data may land anywhere especially on Windows
    expected_dataset = pathlib.Path("_storage").joinpath(dataset_name).resolve()
    assert expected_dataset.exists()
    assert expected_dataset.is_dir()

    # count files in tables
    assert expected_dataset.joinpath(INIT_FILE_NAME).is_file()
    # one numbers table (replaced)
    assert len(list(expected_dataset.joinpath("numbers").glob("*"))) == 1
    # two loads + init
    assert len(list(expected_dataset.joinpath("_dlt_loads").glob("*"))) == 3
    # one schema (dedup on hash)
    assert len(list(expected_dataset.joinpath("_dlt_version").glob("*"))) == 2
    # one state (not sent twice)
    assert len(list(expected_dataset.joinpath("_dlt_pipeline_state").glob("*"))) == 2

    fs_client = pipeline._fs_client()
    # all path formats we use must lead to "_storage" relative to tests
    assert (
        pathlib.Path(fs_client.dataset_path).resolve()
        == pathlib.Path(local_path).joinpath(dataset_name).resolve()
    )
    # same for client
    assert len(fs_client.list_table_files("numbers")) == 1
    assert len(fs_client.list_table_files("_dlt_loads")) == 2
    assert len(fs_client.list_table_files("_dlt_version")) == 1
    assert len(fs_client.list_table_files("_dlt_pipeline_state")) == 1


@pytest.mark.parametrize("truncate", (True, False))
def test_staging_dataset_truncate(truncate) -> None:
    dlt.config["truncate_staging_dataset"] = truncate

    @dlt.resource(write_disposition="merge", merge_key="id")
    def test_data():
        yield [{"field": 1, "id": 1}, {"field": 2, "id": 2}, {"field": 3, "id": 3}]

    pipeline = dlt.pipeline(
        pipeline_name="test_staging_cleared",
        destination="duckdb",
        dev_mode=True,
    )

    info = pipeline.run(test_data, table_name="staging_cleared")
    assert_load_info(info)

    with pipeline.sql_client() as client:
        with client.execute_query(
            f"SELECT * FROM {pipeline.dataset_name}_staging.staging_cleared"
        ) as cur:
            if truncate:
                assert len(cur.fetchall()) == 0
            else:
                assert len(cur.fetchall()) == 3

        with client.execute_query(f"SELECT * FROM {pipeline.dataset_name}.staging_cleared") as cur:
            assert len(cur.fetchall()) == 3


def test_change_naming_convention_name_collision() -> None:
    duck_ = dlt.destinations.duckdb(naming_convention="duck_case", recommended_file_size=120000)
    caps = duck_.capabilities()
    assert caps.naming_convention == "duck_case"
    assert caps.recommended_file_size == 120000

    # use duck case to load data into duckdb so casing and emoji are preserved
    pipeline = dlt.pipeline("test_change_naming_convention_name_collision", destination=duck_)
    info = pipeline.run(
        airtable_emojis().with_resources("📆 Schedule", "🦚Peacock", "🦚WidePeacock")
    )
    assert_load_info(info)
    # make sure that emojis got in
    assert "🦚Peacock" in pipeline.default_schema.tables
    assert "🔑id" in pipeline.default_schema.tables["🦚Peacock"]["columns"]
    assert load_data_table_counts(pipeline) == {
        "📆 Schedule": 3,
        "🦚Peacock": 1,
        "🦚WidePeacock": 1,
        "🦚Peacock__peacock": 3,
        "🦚WidePeacock__Peacock": 3,
    }
    with pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT 🔑id FROM 🦚Peacock")
        # 🔑id value is 1
        assert rows[0][0] == 1

    # change naming convention and run pipeline again so we generate name clashes
    os.environ["SOURCES__AIRTABLE_EMOJIS__SCHEMA__NAMING"] = "sql_ci_v1"
    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(airtable_emojis().with_resources("📆 Schedule", "🦚Peacock", "🦚WidePeacock"))
    # see conflicts early
    assert pip_ex.value.step == "extract"
    assert isinstance(pip_ex.value.__cause__, TableIdentifiersFrozen)

    # all good if we drop tables
    info = pipeline.run(
        airtable_emojis().with_resources("📆 Schedule", "🦚Peacock", "🦚WidePeacock"),
        refresh="drop_resources",
    )
    assert_load_info(info)
    # case insensitive normalization
    assert load_data_table_counts(pipeline) == {
        "_schedule": 3,
        "_peacock": 1,
        "_widepeacock": 1,
        "_peacock__peacock": 3,
        "_widepeacock__peacock": 3,
    }


def test_change_to_more_lax_naming_convention_name_collision() -> None:
    # use snake_case which is strict and then change to duck_case which accepts snake_case names without any changes
    # still we want to detect collisions
    pipeline = dlt.pipeline(
        "test_change_to_more_lax_naming_convention_name_collision", destination="duckdb"
    )
    info = pipeline.run(
        airtable_emojis().with_resources("📆 Schedule", "🦚Peacock", "🦚WidePeacock")
    )
    assert_load_info(info)
    assert "_peacock" in pipeline.default_schema.tables

    # use duck case to load data into duckdb so casing and emoji are preserved
    duck_ = dlt.destinations.duckdb(naming_convention="duck_case")

    # changing destination to one with a separate naming convention raises immediately
    with pytest.raises(TableIdentifiersFrozen):
        pipeline.run(
            airtable_emojis().with_resources("📆 Schedule", "🦚Peacock", "🦚WidePeacock"),
            destination=duck_,
        )

    # refresh on the source level will work
    info = pipeline.run(
        airtable_emojis().with_resources("📆 Schedule", "🦚Peacock", "🦚WidePeacock"),
        destination=duck_,
        refresh="drop_sources",
    )
    assert_load_info(info)
    # make sure that emojis got in
    assert "🦚Peacock" in pipeline.default_schema.tables


def test_change_naming_convention_column_collision() -> None:
    duck_ = dlt.destinations.duckdb(naming_convention="duck_case")

    data = {"Col": "A"}
    pipeline = dlt.pipeline("test_change_naming_convention_column_collision", destination=duck_)
    info = pipeline.run([data], table_name="data")
    assert_load_info(info)

    os.environ["SCHEMA__NAMING"] = "sql_ci_v1"
    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run([data], table_name="data")
    assert isinstance(pip_ex.value.__cause__, TableIdentifiersFrozen)


def test_import_jsonl_file() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_jsonl_import",
        destination="duckdb",
        dev_mode=True,
    )
    columns: List[TColumnSchema] = [
        {"name": "id", "data_type": "bigint", "nullable": False},
        {"name": "name", "data_type": "text"},
        {"name": "description", "data_type": "text"},
        {"name": "ordered_at", "data_type": "date"},
        {"name": "price", "data_type": "decimal"},
    ]
    import_file = "tests/load/cases/loading/header.jsonl"
    info = pipeline.run(
        [dlt.mark.with_file_import(import_file, "jsonl", 2)],
        table_name="no_header",
        loader_file_format="jsonl",
        columns=columns,
    )
    print(info)
    assert_imported_file(pipeline, "no_header", columns, 2)

    # use hints to infer
    hints = dlt.mark.make_hints(columns=columns)
    pipeline.run(
        [dlt.mark.with_file_import(import_file, "jsonl", 2, hints=hints)],
        table_name="no_header_2",
    )
    assert_imported_file(pipeline, "no_header_2", columns, 2, expects_state=False)


def test_import_file_without_sniff_schema() -> None:
    os.environ["RAISE_ON_FAILED_JOBS"] = "false"

    pipeline = dlt.pipeline(
        pipeline_name="test_jsonl_import",
        destination="duckdb",
        dev_mode=True,
    )

    # table will not be found which is terminal exception
    import_file = "tests/load/cases/loading/header.jsonl"
    info = pipeline.run(
        [dlt.mark.with_file_import(import_file, "jsonl", 2)],
        table_name="no_header",
    )
    assert info.has_failed_jobs
    print(info)


def test_import_non_existing_file() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_jsonl_import",
        destination="duckdb",
        dev_mode=True,
    )
    # this file does not exist
    import_file = "tests/load/cases/loading/X_header.jsonl"
    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(
            [dlt.mark.with_file_import(import_file, "jsonl", 2)],
            table_name="no_header",
        )
    inner_ex = pip_ex.value.__cause__
    assert isinstance(inner_ex, FileImportNotFound)
    assert inner_ex.import_file_path == import_file


def test_import_unsupported_file_format() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_jsonl_import",
        destination="duckdb",
        dev_mode=True,
    )
    # this file does not exist
    import_file = "tests/load/cases/loading/csv_no_header.csv"
    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(
            [dlt.mark.with_file_import(import_file, "csv", 2)],
            table_name="no_header",
        )
    inner_ex = pip_ex.value.__cause__
    assert isinstance(inner_ex, NormalizeJobFailed)
    assert isinstance(inner_ex.__cause__, SpecLookupFailed)


def test_import_unknown_file_format() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_jsonl_import",
        destination="duckdb",
        dev_mode=True,
    )
    # this file does not exist
    import_file = "tests/load/cases/loading/csv_no_header.csv"
    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(
            [dlt.mark.with_file_import(import_file, "unknown", 2)],  # type: ignore[arg-type]
            table_name="no_header",
        )
    inner_ex = pip_ex.value.__cause__
    assert isinstance(inner_ex, NormalizeJobFailed)
    # can't figure format from extension
    assert isinstance(inner_ex.__cause__, ValueError)


def test_resource_transformer_standalone() -> None:
    # requires that standalone resources are executes in a single source
    page = 1

    @dlt.resource(name="pages")
    def gen_pages():
        nonlocal page
        while True:
            yield {"page": page}
            if page == 10:
                return
            page += 1

    @dlt.transformer(name="subpages")
    def get_subpages(page_item):
        yield from [
            {
                "page": page_item["page"],
                "subpage": subpage,
            }
            for subpage in range(1, 11)
        ]

    pipeline = dlt.pipeline("test_resource_transformer_standalone", destination="duckdb")
    # here we must combine resources and transformers using the same instance
    info = pipeline.run([gen_pages, gen_pages | get_subpages])
    assert_load_info(info)
    # this works because we extract transformer and resource above in a single source so dlt optimizes
    # dag and extracts gen_pages only once.
    assert load_data_table_counts(pipeline) == {"subpages": 100, "pages": 10}

    # for two separate sources we have the following
    page = 1
    schema = Schema("test")
    info = pipeline.run(
        [DltSource(schema, "", [gen_pages]), DltSource(schema, "", [gen_pages | get_subpages])],
        dataset_name="new_dataset",
    )
    assert_load_info(info, 2)
    # ten subpages because only 1 page is extracted in the second source (see gen_pages exit condition)
    assert load_data_table_counts(pipeline) == {"subpages": 10, "pages": 10}


def test_resources_same_name_in_single_source() -> None:
    source_ids: List[int] = []

    @dlt.resource(name="pages")
    def gen_pages():
        page = 0
        # also store id of current source instance
        source_ids.append(id(dlt.current.source()))
        while True:
            yield {"page": page}
            if page == 10:
                return
            page += 1

    pipeline = dlt.pipeline("test_resources_same_name_in_single_source", destination="duckdb")
    info = pipeline.run([gen_pages(), gen_pages()])
    assert_load_info(info)
    # two separate sources
    assert len(set(source_ids)) == 2

    # check against different names
    source_ids.clear()
    info = pipeline.run([gen_pages().with_name("page_1"), gen_pages().with_name("page_2")])
    assert_load_info(info)
    # one source
    assert len(set(source_ids)) == 1


def test_static_staging_dataset() -> None:
    # share database and staging dataset
    duckdb_ = dlt.destinations.duckdb(
        "_storage/test_static_staging_dataset.db", staging_dataset_name_layout="_dlt_staging"
    )

    pipeline_1 = dlt.pipeline("test_static_staging_dataset_1", destination=duckdb_, dev_mode=True)
    pipeline_2 = dlt.pipeline("test_static_staging_dataset_2", destination=duckdb_, dev_mode=True)
    # staging append (without primary key)
    info = pipeline_1.run([1, 2, 3], table_name="digits", write_disposition="merge")
    assert_load_info(info)
    info = pipeline_2.run(["a", "b", "c", "d"], table_name="letters", write_disposition="merge")
    assert_load_info(info)
    with pipeline_1.sql_client() as client:
        with client.with_alternative_dataset_name("_dlt_staging"):
            assert client.has_dataset()
            schemas = client.execute_sql("SELECT schema_name FROM _dlt_staging._dlt_version")
            assert {s[0] for s in schemas} == {
                "test_static_staging_dataset_1",
                "test_static_staging_dataset_2",
            }

    assert_data_table_counts(pipeline_1, {"digits": 3})
    assert_data_table_counts(pipeline_2, {"letters": 4})


def test_underscore_tables_and_columns() -> None:
    pipeline = dlt.pipeline("test_underscore_tables_and_columns", destination="duckdb")

    @dlt.resource
    def ids(_id=dlt.sources.incremental("_id", initial_value=2)):
        yield from [{"_id": i, "value": l} for i, l in zip([1, 2, 3], ["A", "B", "C"])]

    info = pipeline.run(ids, table_name="_ids")
    assert_load_info(info)
    print(pipeline.default_schema.to_pretty_yaml())
    assert pipeline.last_trace.last_normalize_info.row_counts["_ids"] == 2


def test_dlt_columns_nested_table_collisions() -> None:
    # we generate all identifiers in upper case to test for a bug where dlt columns for nested tables were hardcoded to
    # small caps. they got normalized to upper case after the first run and then added again as small caps
    # generating duplicate columns and raising collision exception as duckdb is ci destination
    duck = duckdb(naming_convention="tests.common.cases.normalizers.sql_upper")
    pipeline = dlt.pipeline("test_dlt_columns_child_table_collisions", destination=duck)
    customers = [
        {"id": 1, "name": "dave", "orders": [1, 2, 3]},
    ]
    assert_load_info(pipeline.run(customers, table_name="CUSTOMERS"))
    # this one would fail without bugfix
    assert_load_info(pipeline.run(customers, table_name="CUSTOMERS"))


def test_access_pipeline_in_resource() -> None:
    pipeline = dlt.pipeline("test_access_pipeline_in_resource", destination="duckdb")

    @dlt.resource(name="user_comments")
    def comments(user_id: str):
        current_pipeline = dlt.current.pipeline()
        # find last comment id for given user_id by looking in destination
        max_id: int = 0
        # on first pipeline run, user_comments table does not yet exist so do not check at all
        # alternatively catch DatabaseUndefinedRelation which is raised when unknown table is selected
        if not current_pipeline.first_run:
            with current_pipeline.sql_client() as client:
                # we may get last user comment or None which we replace with 0
                max_id = (
                    client.execute_sql(
                        "SELECT MAX(_id) FROM user_comments WHERE user_id=?", user_id
                    )[0][0]
                    or 0
                )
        # use max_id to filter our results
        yield from [
            {"_id": i, "value": l, "user_id": user_id}
            for i, l in zip([1, 2, 3], ["A", "B", "C"])
            if i > max_id
        ]

    info = pipeline.run(comments("USER_A"))
    assert_load_info(info)
    assert pipeline.last_trace.last_normalize_info.row_counts["user_comments"] == 3
    info = pipeline.run(comments("USER_A"))
    # no more data for USER_A
    assert_load_info(info, 0)
    info = pipeline.run(comments("USER_B"))
    assert_load_info(info)
    assert pipeline.last_trace.last_normalize_info.row_counts["user_comments"] == 3


def test_exceed_job_file_name_length() -> None:
    # use very long table name both for parent and for a child
    data = {
        "id": 1,
        "child use very long table name both for parent and for a child use very long table name both for parent and for a child use very long table name both for parent and for a child use very long table name both for parent and for a child use very long table name both for parent and for a child": [
            1,
            2,
            3,
        ],
        "col use very long table name both for parent and for a child use very long table name both for parent and for a child use very long table name both for parent and for a child use very long table name both for parent and for a child use very long table name both for parent and for a child": (
            "data"
        ),
    }

    table_name = (
        "parent use very long table name both for parent and for a child use very long table name"
        " both for parent and for a child use very long table name both for parent and for a child"
        " use very long table name both for parent and for a child use very long table name both"
        " for parent and for a child use very long table name both for parent and for a child "
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_exceed_job_file_name_length",
        destination="duckdb",
    )
    # path too long
    with pytest.raises(PipelineStepFailed) as os_err:
        pipeline.run([data], table_name=table_name)
    assert isinstance(os_err.value.__cause__, OSError)

    # fit into 255 + 1
    suffix_len = len(".b61d3af76c.0.insert-values")
    pipeline = dlt.pipeline(
        pipeline_name="test_exceed_job_file_name_length",
        destination=duckdb(
            max_identifier_length=255 - suffix_len + 1,
        ),
    )
    # path too long
    with pytest.raises(PipelineStepFailed):
        pipeline.run([data], table_name=table_name)

    pipeline = dlt.pipeline(
        pipeline_name="test_exceed_job_file_name_length",
        destination=duckdb(
            max_identifier_length=255 - suffix_len,
        ),
    )
    pipeline.run([data], table_name=table_name)


def assert_imported_file(
    pipeline: Pipeline,
    table_name: str,
    columns: List[TColumnSchema],
    expected_rows: int,
    expects_state: bool = True,
) -> None:
    assert_only_table_columns(pipeline, table_name, [col["name"] for col in columns])
    rows = load_tables_to_dicts(pipeline, table_name)
    assert len(rows[table_name]) == expected_rows
    # we should have twp files loaded
    jobs = pipeline.last_trace.last_load_info.load_packages[0].jobs["completed_jobs"]
    job_extensions = [os.path.splitext(job.job_file_info.file_name())[1] for job in jobs]
    assert ".jsonl" in job_extensions
    if expects_state:
        assert ".insert_values" in job_extensions
    # check extract trace if jsonl is really there
    extract_info = pipeline.last_trace.last_extract_info
    jobs = extract_info.load_packages[0].jobs["new_jobs"]
    # find jsonl job
    jsonl_job = next(job for job in jobs if job.job_file_info.table_name == table_name)
    assert jsonl_job.job_file_info.file_format == "jsonl"
    # find metrics for table
    assert (
        extract_info.metrics[extract_info.loads_ids[0]][0]["table_metrics"][table_name].items_count
        == expected_rows
    )


def test_duckdb_column_invalid_timestamp() -> None:
    # DuckDB does not have timestamps with timezone and precision, will default to timezone
    @dlt.resource(
        columns={"event_tstamp": {"data_type": "timestamp", "timezone": True, "precision": 3}},
        primary_key="event_id",
    )
    def events():
        yield [{"event_id": 1, "event_tstamp": "2024-07-30T10:00:00.123+00:00"}]

    pipeline = dlt.pipeline(destination="duckdb")
    pipeline.run(events())
